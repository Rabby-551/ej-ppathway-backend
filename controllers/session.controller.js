import { StatusCodes } from 'http-status-codes';
import mongoose from 'mongoose';
import { Readable } from 'node:stream';
import catchAsync from '../utils/catchAsync.js';
import ApiError from '../utils/ApiError.js';
import sendResponse from '../utils/sendResponse.js';
import { parsePagination, buildMeta } from '../utils/pagination.js';
import Session from '../models/session.model.js';
import SessionSlotLock from '../models/sessionSlotLock.model.js';
import User from '../models/user.model.js';
import Wallet from '../models/wallet.model.js';
import AdvisorProfile from '../models/advisorProfile.model.js';
import Transaction from '../models/transaction.model.js';
import Review from '../models/review.model.js';
import { generateLiveKitToken, createRoom, deleteRoom, startRoomRecording, stopEgress } from '../config/livekit.js';
import { settleSession, chargeUserWallet, refundToUserWallet } from '../services/session.service.js';
import { resolveRecordingUrl } from '../services/recording.service.js';
import { createNotification, broadcastSocket } from '../services/notification.service.js';
import { calculateSessionCredits, getCreditUsage } from '../services/credit.service.js';
import { fetchObjectStorage, parseObjectStorageUrl } from '../services/upload.service.js';
import { recordIapTip } from '../services/iapTip.service.js';

const round2 = (n) => Math.round(n * 100) / 100;
const UNSTARTED_TIMEOUT_STATUSES = ['pending', 'consent', 'waiting', 'scheduled'];
const BOOKING_BLOCKING_STATUSES = ['pending', 'consent', 'waiting', 'scheduled', 'live', 'flagged', 'disputed'];
// Supported mobile durations are all multiples of five minutes. Use that as
// the base grid, then align the visible starts to the selected duration.
const SLOT_BASE_MINUTES = 5;
const WEEKDAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
const SESSION_ASSET_STATUSES = ['completed', 'flagged', 'disputed'];

// Some legacy Bangladesh advisor accounts kept the model's default `UTC`
// value even though their schedule was entered in Bangladesh local time.
// Resolve that known bad default without changing legitimate configured zones.
export const resolveAdvisorTimezone = (user) => {
  const configured = String(user?.timezone || '').trim();
  const country = String(user?.country || '').trim().toUpperCase();
  if ((!configured || configured === 'UTC') && country === 'BD') {
    return 'Asia/Dhaka';
  }
  return configured || 'UTC';
};

const safeDownloadName = (value, fallback) => {
  const cleaned = String(value || '')
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 100);
  return cleaned || fallback;
};

const configuredRecordingHosts = () => {
  const hosts = new Set();
  for (const raw of [
    process.env.RECORDING_PUBLIC_BASE_URL,
    process.env.R2_PUBLIC_BASE_URL,
    process.env.STORAGE_PUBLIC_BASE_URL,
    process.env.EGRESS_S3_PUBLIC_BASE_URL,
    process.env.R2_ENDPOINT,
    process.env.STORAGE_S3_ENDPOINT,
    process.env.EGRESS_S3_ENDPOINT
  ]) {
    if (!raw) continue;
    try {
      hosts.add(new URL(raw).hostname.toLowerCase());
    } catch {
      // Invalid storage configuration is reported as an unavailable download later.
    }
  }
  return hosts;
};

const trustedRecordingUrl = (raw) => {
  if (parseObjectStorageUrl(raw)) return raw;
  try {
    const url = new URL(raw);
    if (!['https:', 'http:'].includes(url.protocol)) return null;
    const host = url.hostname.toLowerCase();
    const trusted =
      configuredRecordingHosts().has(host) ||
      host.endsWith('.amazonaws.com') ||
      host.endsWith('.r2.cloudflarestorage.com') ||
      host.endsWith('.r2.dev');
    return trusted ? url : null;
  } catch {
    return null;
  }
};

const canManageRecording = (user) => {
  if (user.role === 'admin') return true;
  if (user.role !== 'sub_admin') return false;
  const permissions = user.permissions || [];
  return permissions.includes('*') || permissions.includes('recordings.download');
};

const sessionForUserResponse = (session) => {
  const value = typeof session?.toObject === 'function' ? session.toObject() : { ...session };
  value.recordingAvailable = !!value.recordingUrl &&
    !['starting', 'recording', 'failed'].includes(value.recordingStatus);
  delete value.egressId;
  delete value.recordingError;
  if (!value.recordingPriceUnlocked) delete value.recordingUrl;
  if (!value.transcriptPriceUnlocked) delete value.transcriptUrl;
  return value;
};

const createAndBroadcastNotification = async (req, payload, sessionEvent) => {
  const notification = await createNotification(payload);
  const io = req.app.get('io');
  if (io && notification) {
    const data = payload.data || {};
    const socketPayload = {
      _id: String(notification._id),
      type: notification.type,
      title: notification.title,
      body: notification.body,
      data
    };
    broadcastSocket(io, payload.recipient, 'notification:new', socketPayload);
    if (data.sessionId) {
      const sessionPayload = {
        sessionId: String(data.sessionId),
        type: notification.type
      };
      broadcastSocket(io, payload.recipient, 'session:updated', sessionPayload);
      if (sessionEvent) {
        broadcastSocket(io, payload.recipient, sessionEvent, sessionPayload);
      }
    }
  }
  return notification;
};

const scheduledEndTime = (session) => {
  if (!session?.scheduledFor) return null;
  return new Date(new Date(session.scheduledFor).getTime() + (Number(session.durationMinutes) || 0) * 60 * 1000);
};

const liveEndTime = (session) => {
  if (!session?.startedAt) return null;
  return new Date(new Date(session.startedAt).getTime() + (Number(session.durationMinutes) || 0) * 60 * 1000);
};

const isScheduledWindowLive = (session) => {
  if (!session?.scheduledFor || session.startedAt) return false;
  if (!UNSTARTED_TIMEOUT_STATUSES.includes(session.status)) return false;
  const now = new Date();
  const end = scheduledEndTime(session);
  return new Date(session.scheduledFor) <= now && end && end > now;
};

const reconcileTimedOutSession = async (session) => {
  if (!session) return session;
  const now = new Date();

  if (session.status === 'live') {
    const end = liveEndTime(session);
    if (end && end <= now) {
      session.endedAt = session.endedAt || end;
      if (session.egressId) await stopEgress(session.egressId);
      await settleSession(session);
      await session.save();
      if (session.livekitRoom) await deleteRoom(session.livekitRoom);
    }
    return session;
  }

  if (!session.startedAt && UNSTARTED_TIMEOUT_STATUSES.includes(session.status)) {
    const end = scheduledEndTime(session);
    if (end && end <= now) {
      session.status = 'expired';
      session.endedAt = session.endedAt || end;
      await session.save();
      if (session.livekitRoom) await deleteRoom(session.livekitRoom);
    }
  }

  return session;
};

const reconcileTimedOutSessions = async (sessions) => {
  return Promise.all(sessions.map((session) => reconcileTimedOutSession(session)));
};

const pad2 = (value) => String(value).padStart(2, '0');

const toMinutes = (hhmm) => {
  if (!hhmm || typeof hhmm !== 'string') return null;
  const [hour, minute] = hhmm.split(':').map((part) => Number.parseInt(part, 10));
  if (Number.isNaN(hour) || Number.isNaN(minute)) return null;
  return hour * 60 + minute;
};

const previousWeekday = (weekday) => {
  const index = WEEKDAYS.indexOf(weekday);
  return WEEKDAYS[(index + 6) % 7] || weekday;
};

const scheduleSlots = (day) => {
  if (!day || day.enabled === false) return [];
  const rawSlots = Array.isArray(day.slots) && day.slots.length
    ? day.slots
    : [{ from: day.from, to: day.to }];
  return rawSlots
    .map((slot) => ({
      from: slot?.from || '09:00',
      to: slot?.to || '18:00',
      fromMinutes: toMinutes(slot?.from || '09:00'),
      toMinutes: toMinutes(slot?.to || '18:00')
    }))
    .filter((slot) => slot.fromMinutes !== null && slot.toMinutes !== null);
};

const slotContainsMinute = (slot, minuteOfDay, { previousDay = false } = {}) => {
  if (slot.toMinutes <= slot.fromMinutes) {
    return previousDay
      ? minuteOfDay < slot.toMinutes
      : minuteOfDay >= slot.fromMinutes;
  }
  return !previousDay && minuteOfDay >= slot.fromMinutes && minuteOfDay < slot.toMinutes;
};

const matchingScheduleSlot = (weeklySchedule, date, timezone) => {
  if (!weeklySchedule) return null;
  const parts = zonedParts(date, timezone);
  const minuteOfDay = parts.hour * 60 + parts.minute;
  const daySlots = scheduleSlots(weeklySchedule[parts.weekday]);
  const currentSlot = daySlots.find((slot) => slotContainsMinute(slot, minuteOfDay));
  if (currentSlot) return { weekday: parts.weekday, ...currentSlot };

  const prevWeekday = previousWeekday(parts.weekday);
  const previousSlot = scheduleSlots(weeklySchedule[prevWeekday]).find((slot) =>
    slot.toMinutes <= slot.fromMinutes && slotContainsMinute(slot, minuteOfDay, { previousDay: true })
  );
  return previousSlot ? { weekday: prevWeekday, ...previousSlot } : null;
};

export const isDurationAlignedStart = (date, timezone, slot, durationMinutes) => {
  if (!slot || durationMinutes <= 0) return false;
  const parts = zonedParts(date, timezone);
  const minuteOfDay = parts.hour * 60 + parts.minute;
  let minutesFromWindowStart = minuteOfDay - slot.fromMinutes;
  if (slot.toMinutes <= slot.fromMinutes && minuteOfDay < slot.toMinutes) {
    minutesFromWindowStart += 24 * 60;
  }
  return minutesFromWindowStart >= 0 && minutesFromWindowStart % durationMinutes === 0;
};

const scheduleWindowsForDay = (weeklySchedule, weekday) => scheduleSlots(weeklySchedule?.[weekday])
  .map(({ from, to }) => ({ from, to }));

const dateAvailabilityEntry = (dateAvailability, dateKey) => {
  if (!dateAvailability || !dateKey) return null;
  if (dateAvailability instanceof Map) return dateAvailability.get(dateKey) || null;
  return dateAvailability[dateKey] || null;
};

// A date rule overrides the weekly schedule only when it explicitly marks the
// day unavailable or defines its own slots. An empty/stale entry (no slots and
// not marked unavailable) is ignored so the advisor's regular weekly hours apply.
const dateOverrideActive = (entry) =>
  !!entry && (entry.unavailable === true || (Array.isArray(entry.slots) && entry.slots.length > 0));

const dateAvailabilitySlots = (day) => {
  if (!day || day.unavailable === true) return [];
  return (Array.isArray(day.slots) ? day.slots : [])
    .map((slot) => ({
      from: slot?.from,
      to: slot?.to,
      fromMinutes: toMinutes(slot?.from),
      toMinutes: toMinutes(slot?.to)
    }))
    .filter((slot) => slot.fromMinutes !== null && slot.toMinutes !== null);
};

const matchingAvailabilitySlot = (profile, date, timezone) => {
  const parts = zonedParts(date, timezone);
  const dateKey = localDateKey(parts);
  const currentDay = dateAvailabilityEntry(profile?.dateAvailability, dateKey);
  if (dateOverrideActive(currentDay)) {
    if (currentDay.unavailable === true) return null;
    const minuteOfDay = parts.hour * 60 + parts.minute;
    const currentSlot = dateAvailabilitySlots(currentDay).find((slot) =>
      slotContainsMinute(slot, minuteOfDay)
    );
    if (currentSlot) return { date: dateKey, ...currentSlot };
    return null;
  }

  const prevDate = new Date(date.getTime() - 24 * 60 * 60 * 1000);
  const prevKey = localDateKey(zonedParts(prevDate, timezone));
  const previousDay = dateAvailabilityEntry(profile?.dateAvailability, prevKey);
  if (dateOverrideActive(previousDay) && previousDay.unavailable !== true) {
    const minuteOfDay = parts.hour * 60 + parts.minute;
    const previousSlot = dateAvailabilitySlots(previousDay).find((slot) =>
      slot.toMinutes <= slot.fromMinutes && slotContainsMinute(slot, minuteOfDay, { previousDay: true })
    );
    if (previousSlot) return { date: prevKey, ...previousSlot };
  }

  return matchingScheduleSlot(profile?.weeklySchedule, date, timezone);
};

const availabilityForDate = (profile, dateKey, weekday) => {
  const dateRule = dateAvailabilityEntry(profile?.dateAvailability, dateKey);
  if (dateOverrideActive(dateRule)) {
    const windows = dateRule.unavailable === true
      ? []
      : dateAvailabilitySlots(dateRule).map(({ from, to }) => ({ from, to }));
    return {
      scheduleForDay: dateRule.unavailable === true ? null : { enabled: true, slots: dateRule.slots },
      scheduleWindows: windows
    };
  }
  const scheduleForDay = profile?.weeklySchedule?.[weekday] || null;
  return {
    scheduleForDay,
    scheduleWindows: scheduleWindowsForDay(profile?.weeklySchedule, weekday)
  };
};

const zonedParts = (date, timezone = 'UTC') => {
  let formatter;
  try {
    formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone || 'UTC',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      weekday: 'long',
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23'
    });
  } catch {
    formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: 'UTC',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      weekday: 'long',
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23'
    });
  }
  const parts = formatter.formatToParts(date);
  const get = (type) => parts.find((part) => part.type === type)?.value;
  return {
    year: get('year'),
    month: get('month'),
    day: get('day'),
    weekday: (get('weekday') || '').toLowerCase(),
    hour: Number.parseInt(get('hour') || '0', 10),
    minute: Number.parseInt(get('minute') || '0', 10)
  };
};

const localDateKey = (parts) => `${parts.year}-${parts.month}-${parts.day}`;

const formatTimeInZone = (date, timezone = 'UTC') => {
  let formatter;
  try {
    formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone || 'UTC',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  } catch {
    formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: 'UTC',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  }
  return formatter.format(date);
};

const fixedOffsetParts = (date, offsetMinutes = 0) => {
  const shifted = new Date(date.getTime() + offsetMinutes * 60 * 1000);
  return {
    year: String(shifted.getUTCFullYear()),
    month: pad2(shifted.getUTCMonth() + 1),
    day: pad2(shifted.getUTCDate()),
    hour: shifted.getUTCHours(),
    minute: shifted.getUTCMinutes()
  };
};

const formatTimeWithOffset = (date, offsetMinutes = 0) => {
  const parts = fixedOffsetParts(date, offsetMinutes);
  const suffix = parts.hour >= 12 ? 'PM' : 'AM';
  const hour12 = parts.hour % 12 === 0 ? 12 : parts.hour % 12;
  return `${hour12}:${pad2(parts.minute)} ${suffix}`;
};

const parseOffsetMinutes = (value) => {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return null;
  if (parsed < -14 * 60 || parsed > 14 * 60) return null;
  return parsed;
};

const viewerDateParts = (date, { timezone, offsetMinutes }) => {
  if (offsetMinutes !== null && offsetMinutes !== undefined) {
    return fixedOffsetParts(date, offsetMinutes);
  }
  return zonedParts(date, timezone || 'UTC');
};

const formatViewerTime = (date, { timezone, offsetMinutes }) => {
  if (offsetMinutes !== null && offsetMinutes !== undefined) {
    return formatTimeWithOffset(date, offsetMinutes);
  }
  return formatTimeInZone(date, timezone || 'UTC');
};

const isWithinDaySchedule = (weeklySchedule, date, timezone) => {
  return !!matchingScheduleSlot(weeklySchedule, date, timezone);
};

const sessionRange = (session) => {
  if (!session?.scheduledFor) return null;
  const start = new Date(session.scheduledFor);
  const end = new Date(start.getTime() + (Number(session.durationMinutes) || 0) * 60 * 1000);
  return { start, end };
};

const rangesOverlap = (startA, endA, startB, endB) => startA < endB && endA > startB;

const availabilitySettings = (profile) => ({
  minNoticeMinutes: Math.max(0, Number(profile?.availabilitySettings?.minNoticeMinutes) || 0),
  bookingWindowDays: Math.max(1, Number(profile?.availabilitySettings?.bookingWindowDays) || 30),
  bufferMinutes: Math.max(0, Number(profile?.availabilitySettings?.bufferMinutes) || 0),
  defaultDurationMinutes: Math.max(1, Number(profile?.availabilitySettings?.defaultDurationMinutes) || 15),
  sameDayBooking: profile?.availabilitySettings?.sameDayBooking !== false
});

const findBlockingBookings = async ({ advisorId, start, end, excludeSessionId, bufferMinutes = 0 }) => {
  const bufferMs = Math.max(0, Number(bufferMinutes) || 0) * 60 * 1000;
  const blockedStart = new Date(start.getTime() - bufferMs);
  const blockedEnd = new Date(end.getTime() + bufferMs);
  const windowStart = new Date(blockedStart.getTime() - 24 * 60 * 60 * 1000);
  const windowEnd = new Date(blockedEnd.getTime() + 24 * 60 * 60 * 1000);
  const filter = {
    advisor: advisorId,
    status: { $in: BOOKING_BLOCKING_STATUSES },
    scheduledFor: { $gte: windowStart, $lte: windowEnd }
  };
  if (excludeSessionId) filter._id = { $ne: excludeSessionId };
  const sessions = await Session.find(filter)
    .populate('user', 'name profilePhoto')
    .sort({ scheduledFor: 1 });
  return sessions.filter((session) => {
    const range = sessionRange(session);
    return range && rangesOverlap(blockedStart, blockedEnd, range.start, range.end);
  });
};

const assertAdvisorSlotAvailable = async ({ advisorId, profile, start, durationMinutes, excludeSessionId }) => {
  if (!start || Number.isNaN(start.getTime())) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'Invalid scheduled time');
  }
  const duration = Math.max(1, Number(durationMinutes) || 15);
  const end = new Date(start.getTime() + duration * 60 * 1000);
  const settings = availabilitySettings(profile);
  const now = new Date();
  const minStart = new Date(now.getTime() + settings.minNoticeMinutes * 60 * 1000);
  const maxStart = new Date(now.getTime() + settings.bookingWindowDays * 24 * 60 * 60 * 1000);
  if (start < minStart) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'This time is inside the advisor minimum notice window');
  }
  if (start > maxStart) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'This time is outside the advisor booking window');
  }
  const timezone = resolveAdvisorTimezone(profile?.user || profile);
  if (!settings.sameDayBooking && localDateKey(zonedParts(start, timezone)) === localDateKey(zonedParts(now, timezone))) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'Same-day booking is not available for this advisor');
  }
  const startSlot = matchingAvailabilitySlot(profile, start, timezone);
  const endSlot = matchingAvailabilitySlot(profile, new Date(end.getTime() - 60 * 1000), timezone);
  if (!startSlot || !endSlot || (startSlot.weekday || startSlot.date) !== (endSlot.weekday || endSlot.date) || startSlot.from !== endSlot.from || startSlot.to !== endSlot.to) {
    throw new ApiError(StatusCodes.CONFLICT, 'Advisor is not available at this time');
  }
  const conflicts = await findBlockingBookings({ advisorId, start, end, excludeSessionId, bufferMinutes: settings.bufferMinutes });
  if (conflicts.length) {
    throw new ApiError(StatusCodes.CONFLICT, 'This time is already booked. Please choose another available slot.');
  }
};

const assertAdvisorSessionTypeEnabled = (profile, type) => {
  const sessionTypes = profile?.sessionTypes || {};
  if (sessionTypes[type] === false) {
    throw new ApiError(StatusCodes.CONFLICT, `Advisor is not available for ${type} sessions`);
  }
};

export const lockStartsForRange = (start, durationMinutes) => {
  const slots = [];
  const duration = Math.max(1, Number(durationMinutes) || 15);
  const stepMs = SLOT_BASE_MINUTES * 60 * 1000;
  const endMs = start.getTime() + duration * 60 * 1000;
  const firstSlotMs = Math.floor(start.getTime() / stepMs) * stepMs;
  for (let ms = firstSlotMs; ms < endMs; ms += stepMs) {
    slots.push(new Date(ms));
  }
  return slots;
};

const reserveSlotLocks = async ({ advisorId, sessionId, start, durationMinutes }) => {
  const docs = lockStartsForRange(start, durationMinutes).map((slotStart) => ({
    advisor: advisorId,
    session: sessionId,
    slotStart
  }));
  try {
    await SessionSlotLock.insertMany(docs, { ordered: true });
  } catch (error) {
    if (error?.code === 11000 || error?.writeErrors?.some((item) => item?.code === 11000)) {
      throw new ApiError(StatusCodes.CONFLICT, 'This time is already booked. Please choose another available slot.');
    }
    throw error;
  }
};

const releaseSlotLocks = (sessionId) => SessionSlotLock.deleteMany({ session: sessionId });

export const buildAdvisorAvailability = async ({ advisorId, date, durationMinutes, viewerTimezone, viewerOffsetMinutes }) => {
  const advisor = await User.findOne({ _id: advisorId, role: 'advisor' }).select('name timezone country status');
  if (!advisor) throw new ApiError(StatusCodes.NOT_FOUND, 'Advisor not found');
  const profile = await AdvisorProfile.findOne({ user: advisorId }).populate('user', 'timezone country');
  if (!profile) throw new ApiError(StatusCodes.NOT_FOUND, 'Advisor profile missing');

  const settings = availabilitySettings(profile);
  const duration = Math.max(1, Number(durationMinutes) || settings.defaultDurationMinutes);
  const viewer = {
    timezone: viewerTimezone || resolveAdvisorTimezone(advisor),
    offsetMinutes: viewerOffsetMinutes
  };
  const dateKey = /^\d{4}-\d{2}-\d{2}$/.test(date || '') ? date : localDateKey(viewerDateParts(new Date(), viewer));
  const [year, month, day] = dateKey.split('-').map((part) => Number.parseInt(part, 10));
  const timezone = resolveAdvisorTimezone(advisor);
  const probeDate = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  const weekday = zonedParts(probeDate, timezone).weekday;
  const { scheduleForDay, scheduleWindows } = availabilityForDate(profile, dateKey, weekday);
  const searchStart = new Date(Date.UTC(year, month - 1, day - 1, 0, 0, 0));
  const searchEnd = new Date(Date.UTC(year, month - 1, day + 2, 0, 0, 0));
  const now = new Date();
  const minStart = new Date(now.getTime() + settings.minNoticeMinutes * 60 * 1000);
  const maxStart = new Date(now.getTime() + settings.bookingWindowDays * 24 * 60 * 60 * 1000);

  const bookedDocs = await Session.find({
    advisor: advisorId,
    status: { $in: BOOKING_BLOCKING_STATUSES },
    scheduledFor: { $gte: searchStart, $lt: searchEnd }
  })
    .populate('user', 'name profilePhoto')
    .sort({ scheduledFor: 1 });

  const bookedSlots = bookedDocs
    .map((session) => {
      const range = sessionRange(session);
      if (!range) return null;
      const parts = viewerDateParts(range.start, viewer);
      if (localDateKey(parts) !== dateKey) return null;
      return {
        sessionId: String(session._id),
        start: range.start.toISOString(),
        end: range.end.toISOString(),
        startLabel: formatViewerTime(range.start, viewer),
        endLabel: formatViewerTime(range.end, viewer),
        durationMinutes: session.durationMinutes,
        type: session.type,
        status: session.status
      };
    })
    .filter(Boolean);

  const availableSlots = [];
  for (let ms = searchStart.getTime(); ms < searchEnd.getTime(); ms += SLOT_BASE_MINUTES * 60 * 1000) {
    const start = new Date(ms);
    const end = new Date(ms + duration * 60 * 1000);
    if (start < minStart || start > maxStart) continue;
    if (!settings.sameDayBooking && localDateKey(zonedParts(start, timezone)) === localDateKey(zonedParts(now, timezone))) continue;
    if (localDateKey(viewerDateParts(start, viewer)) !== dateKey) continue;
    const startSlot = matchingAvailabilitySlot(profile, start, timezone);
    const endSlot = matchingAvailabilitySlot(profile, new Date(end.getTime() - 60 * 1000), timezone);
    if (!startSlot || !endSlot || (startSlot.weekday || startSlot.date) !== (endSlot.weekday || endSlot.date) || startSlot.from !== endSlot.from || startSlot.to !== endSlot.to) continue;
    if (!isDurationAlignedStart(start, timezone, startSlot, duration)) continue;
    const overlaps = bookedDocs.some((session) => {
      const range = sessionRange(session);
      return range && rangesOverlap(
        new Date(start.getTime() - settings.bufferMinutes * 60 * 1000),
        new Date(end.getTime() + settings.bufferMinutes * 60 * 1000),
        range.start,
        range.end
      );
    });
    if (!overlaps) {
      availableSlots.push({
        start: start.toISOString(),
        end: end.toISOString(),
        startLabel: formatViewerTime(start, viewer),
        endLabel: formatViewerTime(end, viewer),
        durationMinutes: duration
      });
    }
  }

  if (!availableSlots.length) {
    console.log('[availability:empty]', {
      advisorId: String(advisor._id),
      dateKey,
      weekday,
      scheduleTimezone: timezone,
      viewerTimezone: viewer.timezone,
      viewerOffsetMinutes: viewer.offsetMinutes ?? null,
      now: now.toISOString(),
      scheduleForDay,
      scheduleWindows,
      dateOverride: dateOverrideActive(dateAvailabilityEntry(profile?.dateAvailability, dateKey))
    });
  }

  return {
    advisorId: String(advisor._id),
    advisorName: advisor.name,
    timezone,
    displayTimezone: viewer.timezone,
    displayTimezoneOffsetMinutes: viewer.offsetMinutes,
    date: dateKey,
    durationMinutes: duration,
    stepMinutes: duration,
    sessionTypes: {
      chat: profile.sessionTypes?.chat !== false,
      call: profile.sessionTypes?.call !== false,
      video: profile.sessionTypes?.video !== false
    },
    scheduleWindow: scheduleForDay && scheduleForDay.enabled !== false
      ? {
          from: scheduleWindows[0]?.from || scheduleForDay.from,
          to: scheduleWindows[0]?.to || scheduleForDay.to
        }
      : null,
    scheduleWindows: scheduleForDay && scheduleForDay.enabled !== false ? scheduleWindows : [],
    availableSlots,
    bookedSlots
  };
};

export const advisorAvailability = catchAsync(async (req, res) => {
  const data = await buildAdvisorAvailability({
    advisorId: req.params.advisorId,
    date: req.query.date,
    durationMinutes: req.query.durationMinutes,
    viewerTimezone: req.query.timezone,
    viewerOffsetMinutes: parseOffsetMinutes(req.query.timezoneOffsetMinutes)
  });
  return sendResponse(res, { data });
});

// ============= Booking =============
export const createBooking = catchAsync(async (req, res) => {
  const { advisorId, type, scheduledFor, durationMinutes, instantStart } = req.body;

  if (!['chat', 'call', 'video'].includes(type)) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'Invalid session type');
  }

  const advisor = await User.findOne({ _id: advisorId, role: 'advisor' });
  if (!advisor) throw new ApiError(StatusCodes.NOT_FOUND, 'Advisor not found');
  if (advisor.status !== 'active') throw new ApiError(StatusCodes.FORBIDDEN, 'Advisor not available');

  const profile = await AdvisorProfile.findOne({ user: advisorId }).populate('user', 'timezone country');
  if (!profile) throw new ApiError(StatusCodes.NOT_FOUND, 'Advisor profile missing');
  assertAdvisorSessionTypeEnabled(profile, type);

  const duration = Math.max(1, Number(durationMinutes) || 15);
  const scheduledStart = instantStart ? new Date() : scheduledFor ? new Date(scheduledFor) : new Date();
  await assertAdvisorSlotAvailable({
    advisorId,
    profile,
    start: scheduledStart,
    durationMinutes: duration
  });

  const { ratePerMin, credits: estimatedCost } = await calculateSessionCredits({
    profile,
    type,
    durationMinutes: duration
  });
  const usage = await getCreditUsage();

  // verify wallet has enough for estimated cost
  const wallet = await Wallet.findOne({ user: req.user._id });
  if (!wallet) throw new ApiError(StatusCodes.BAD_REQUEST, 'Wallet not found');
  if ((wallet.balance + wallet.freeCredits) < estimatedCost) {
    throw new ApiError(StatusCodes.PAYMENT_REQUIRED, 'Insufficient wallet balance to book');
  }

  const sessionId = new mongoose.Types.ObjectId();
  await reserveSlotLocks({
    advisorId,
    sessionId,
    start: scheduledStart,
    durationMinutes: duration
  });

  let session;
  try {
    const recordingUnlockCharge =
      type === 'video'
        ? usage.videoRecording ?? usage.sessionRecording
        : type === 'call'
          ? usage.audioRecording ?? usage.sessionRecording
          : usage.sessionRecording;
    session = await Session.create({
    _id: sessionId,
    user: req.user._id,
    advisor: advisorId,
    type,
    status: 'pending',
    scheduledFor: scheduledStart,
    durationMinutes: duration,
    instantStart: !!instantStart,
    ratePerMin,
    estimatedCost,
    holdAmount: estimatedCost,
    unlockChargeRecording: recordingUnlockCharge,
    unlockChargeTranscript: usage.chatTranscript
  });
  } catch (error) {
    await releaseSlotLocks(sessionId);
    throw error;
  }

  await createAndBroadcastNotification(req, {
    recipient: advisorId,
    type: 'session_request',
    title: 'New session request',
    body: `${req.user.name} requested a ${type} session`,
    data: { sessionId: session._id }
  }, 'session:created');
  await createAndBroadcastNotification(req, {
    recipient: req.user._id,
    type: 'session_confirmed',
    title: 'Booking confirmed',
    body: `Your ${type} session is booked`,
    data: { sessionId: session._id }
  }, 'session:created');

  return sendResponse(res, {
    statusCode: StatusCodes.CREATED,
    message: 'Session booked',
    data: session
  });
});

// ============= List sessions (user) =============
export const myUserSessions = catchAsync(async (req, res) => {
  const { skip, limit, page } = parsePagination(req.query);
  const filter = { user: req.user._id };

  const tab = req.query.tab; // all|today|upcoming|completed|canceled
  const now = new Date();
  if (tab === 'today') {
    const start = new Date(); start.setHours(0,0,0,0);
    const end = new Date(); end.setHours(23,59,59,999);
    filter.scheduledFor = { $gte: start, $lte: end };
  } else if (tab === 'upcoming') {
    filter.scheduledFor = { $gte: now };
    filter.status = 'pending';
  } else if (tab === 'completed') {
    filter.status = 'completed';
  } else if (tab === 'canceled') {
    filter.status = 'cancelled';
  }

  const total = await Session.countDocuments(filter);
  const docs = await Session.find(filter)
    .populate('advisor', 'name profilePhoto')
    .sort({ scheduledFor: -1, createdAt: -1 })
    .skip(skip).limit(limit);
  const reconciled = await reconcileTimedOutSessions(docs);
  const items = reconciled.map(sessionForUserResponse);

  return sendResponse(res, { data: items, meta: buildMeta({ page, limit, total }) });
});

// ============= List sessions (advisor) =============
export const myAdvisorSessions = catchAsync(async (req, res) => {
  const { skip, limit, page } = parsePagination(req.query);
  const filter = { advisor: req.user._id };
  const now = new Date();

  const tab = req.query.tab; // all|live|upcoming|completed|cancelled|canceled|disputed|flagged
  if (tab === 'live') {
    filter.$or = [
      { status: 'live' },
      { status: { $in: UNSTARTED_TIMEOUT_STATUSES }, scheduledFor: { $lte: now } }
    ];
  }
  else if (tab === 'upcoming') {
    filter.status = { $in: ['pending', 'consent', 'waiting'] };
    filter.scheduledFor = { $gte: now };
  }
  else if (tab === 'completed') filter.status = 'completed';
  else if (tab === 'cancelled' || tab === 'canceled') filter.status = 'cancelled';
  else if (tab === 'disputed') filter.status = 'disputed';
  else if (tab === 'flagged') filter.status = 'flagged';

  const docs = await Session.find(filter)
    .populate('user', 'name profilePhoto')
    .sort({ createdAt: -1 })
    .skip(skip).limit(limit);
  const reconciled = await reconcileTimedOutSessions(docs);
  const items = reconciled
    .filter((s) => tab !== 'live' || s.status === 'live' || isScheduledWindowLive(s))
    .map((s) => s.toObject());
  const total = tab === 'live' ? items.length : await Session.countDocuments(filter);

  // Attach rating for completed sessions in a single batched query.
  const completedIds = items
    .filter((s) => s.status === 'completed')
    .map((s) => s._id);
  if (completedIds.length) {
    const reviews = await Review.find({ session: { $in: completedIds } })
      .select('session rating')
      .lean();
    const ratingBySession = new Map(
      reviews.map((r) => [String(r.session), r.rating])
    );
    for (const s of items) {
      const r = ratingBySession.get(String(s._id));
      if (typeof r === 'number') s.rating = r;
    }
  }

  return sendResponse(res, { data: items, meta: buildMeta({ page, limit, total }) });
});

// ============= Bookings (calendar) for advisor =============
export const advisorBookingsCalendar = catchAsync(async (req, res) => {
  const { from, to } = req.query;
  const filter = { advisor: req.user._id };
  if (from || to) filter.scheduledFor = {};
  if (from) filter.scheduledFor.$gte = new Date(from);
  if (to) filter.scheduledFor.$lte = new Date(to);

  const docs = await Session.find(filter)
    .populate('user', 'name profilePhoto')
    .sort({ scheduledFor: 1 });
  const reconciled = await reconcileTimedOutSessions(docs);
  const items = reconciled.map((s) => s.toObject());

  return sendResponse(res, { data: items });
});

// ============= Session Details =============
export const getSession = catchAsync(async (req, res) => {
  const sessionDoc = await Session.findById(req.params.id)
    .populate('user', 'name profilePhoto')
    .populate('advisor', 'name profilePhoto');
  if (!sessionDoc) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  const isUser = String(sessionDoc.user?._id || sessionDoc.user) === String(req.user._id);
  const isAdvisor = String(sessionDoc.advisor?._id || sessionDoc.advisor) === String(req.user._id);
  if (!isUser && !isAdvisor && !['admin', 'sub_admin'].includes(req.user.role)) {
    throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');
  }
  const session = await reconcileTimedOutSession(sessionDoc);
  return sendResponse(res, {
    data: isUser ? sessionForUserResponse(session) : session.toObject()
  });
});

// ============= Download completed call/video recording =============
export const downloadSessionRecording = catchAsync(async (req, res) => {
  if (!mongoose.isValidObjectId(req.params.id)) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'Invalid session ID');
  }

  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (!['call', 'video'].includes(session.type)) {
    throw new ApiError(StatusCodes.BAD_REQUEST, 'Text sessions do not have media recordings');
  }

  const isUser = String(session.user) === String(req.user._id);
  const isAdvisor = String(session.advisor) === String(req.user._id);
  const isAdmin = canManageRecording(req.user);
  if (!isUser && !isAdvisor && !isAdmin) {
    throw new ApiError(StatusCodes.FORBIDDEN, 'You cannot access this recording');
  }
  if (isUser && !session.recordingPriceUnlocked) {
    throw new ApiError(StatusCodes.PAYMENT_REQUIRED, 'Unlock this recording before downloading it');
  }
  const isStillProcessing =
    session.status === 'live' ||
    session.recordingStatus === 'starting' ||
    (session.recordingStatus === 'recording' && !session.recordingUrl);
  if (isStillProcessing) {
    throw new ApiError(StatusCodes.CONFLICT, 'The recording is still being processed');
  }
  if (!session.recordingUrl || session.recordingStatus === 'failed') {
    throw new ApiError(StatusCodes.NOT_FOUND, 'Recording is not available');
  }

  const trustedRecording = trustedRecordingUrl(session.recordingUrl);
  if (!trustedRecording) {
    console.error('Blocked untrusted or unusable recording URL', {
      sessionId: String(session._id),
      recordingUrl: session.recordingUrl
    });
    throw new ApiError(StatusCodes.SERVICE_UNAVAILABLE, 'Recording storage is not available');
  }

  let upstream;
  const controller = new AbortController();
  const connectTimeout = setTimeout(() => controller.abort(), 30_000);
  try {
    upstream = parseObjectStorageUrl(trustedRecording)
      ? await fetchObjectStorage(trustedRecording)
      : await fetch(trustedRecording, { signal: controller.signal, redirect: 'error' });
  } catch (error) {
    console.error('Recording download fetch failed', {
      sessionId: String(session._id),
      message: error?.message
    });
    throw new ApiError(StatusCodes.BAD_GATEWAY, 'Could not retrieve the recording');
  } finally {
    clearTimeout(connectTimeout);
  }
  if (!upstream.ok || !upstream.body) {
    console.error('Recording storage returned an error', {
      sessionId: String(session._id),
      status: upstream.status
    });
    throw new ApiError(StatusCodes.BAD_GATEWAY, 'Could not retrieve the recording');
  }

  const filename = safeDownloadName(
    `session-recording-${session.sessionCode || session._id}.mp4`,
    'session-recording.mp4'
  );
  res.setHeader('Content-Type', upstream.headers.get('content-type') || 'video/mp4');
  res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
  const length = upstream.headers.get('content-length');
  if (length) res.setHeader('Content-Length', length);
  res.setHeader('Cache-Control', 'private, no-store');

  const stream = Readable.fromWeb(upstream.body);
  req.on('aborted', () => stream.destroy());
  stream.on('error', (error) => {
    console.error('Recording download stream failed', {
      sessionId: String(session._id),
      message: error?.message
    });
    if (!res.headersSent) res.status(StatusCodes.BAD_GATEWAY).end();
    else res.destroy(error);
  });
  stream.pipe(res);
});

// ============= Recording consent (user) =============
export const consentRecording = catchAsync(async (req, res) => {
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (String(session.user) !== String(req.user._id)) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');

  session.recordingConsented = true;
  if (session.status === 'pending') session.status = 'consent';
  await session.save();
  return sendResponse(res, { message: 'Consent recorded', data: session });
});

// ============= Get LiveKit token (user/advisor) =============
export const getLiveKitToken = catchAsync(async (req, res) => {
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');

  const isUser = String(session.user) === String(req.user._id);
  const isAdvisor = String(session.advisor) === String(req.user._id);
  if (!isUser && !isAdvisor) throw new ApiError(StatusCodes.FORBIDDEN, 'Not a participant');

  const roomName = session.livekitRoom || `session_${session._id}`;
  if (!session.livekitRoom) {
    session.livekitRoom = roomName;
    await createRoom(roomName, { maxParticipants: 2, metadata: { sessionId: String(session._id) } });
    await session.save();
  }

  // Mark waiting room joined for user
  if (isUser && !session.userJoinedAt) {
    session.userJoinedAt = new Date();
    if (session.status === 'consent' || session.status === 'pending') {
      session.status = 'waiting';
      session.waitingStartedAt = new Date();
    }
    await session.save();
  }
  if (isAdvisor && !session.advisorJoinedAt) {
    session.advisorJoinedAt = new Date();
    await session.save();
  }

  const { token, url } = await generateLiveKitToken({
    identity: String(req.user._id),
    name: req.user.name,
    roomName,
    metadata: { role: isAdvisor ? 'advisor' : 'user', sessionId: String(session._id) }
  });

  return sendResponse(res, { data: { token, url, roomName } });
});

// ============= Advisor starts session =============
export const advisorStartSession = catchAsync(async (req, res) => {
  let session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (String(session.advisor) !== String(req.user._id)) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');
  session = await reconcileTimedOutSession(session);
  if (session.status === 'expired') {
    return sendResponse(res, { message: 'Session time expired', data: session.toObject() });
  }

  if (session.status === 'live') return sendResponse(res, { data: session });
  if (!['waiting', 'consent', 'pending'].includes(session.status))
    throw new ApiError(StatusCodes.BAD_REQUEST, `Cannot start session in status ${session.status}`);

  // pre-charge a small hold for first minute (cap at remaining wallet)
  const holdAmount = Math.min(session.holdAmount || session.estimatedCost || 0, Math.ceil(session.ratePerMin * 1));
  if (holdAmount > 0) {
    try {
      const { creditsUsed, balanceUsed } = await chargeUserWallet({ userId: session.user, amount: holdAmount });
      session.creditsUsed = round2((session.creditsUsed || 0) + creditsUsed);
      session.chargedAmount = round2((session.chargedAmount || 0) + creditsUsed + balanceUsed);
      await Transaction.create({
        type: 'session_charge',
        status: 'completed',
        user: session.user,
        advisor: session.advisor,
        session: session._id,
        amount: round2(creditsUsed + balanceUsed),
        description: `Initial hold for session ${session.sessionCode}`
      });
    } catch (e) {
      throw new ApiError(StatusCodes.PAYMENT_REQUIRED, 'User has insufficient funds');
    }
  }

  session.status = 'live';
  session.startedAt = new Date();
  await session.save();

  // Call and video sessions are always recorded once they are live. The atomic
  // claim prevents overlapping advisor-start requests from creating two egress jobs.
  if (session.type === 'call' || session.type === 'video') {
    const claimed = await Session.findOneAndUpdate(
      {
        _id: session._id,
        status: 'live',
        type: { $in: ['call', 'video'] },
        $or: [{ egressId: { $exists: false } }, { egressId: null }, { egressId: '' }],
        recordingStatus: { $nin: ['starting', 'recording', 'completed'] }
      },
      { $set: { recordingStatus: 'starting', recordingError: '' } },
      { new: true }
    );

    if (claimed) {
      const roomName = claimed.livekitRoom || `session_${claimed._id}`;
      const fileName = `${claimed._id}-${Date.now()}.mp4`;
      const egress = await startRoomRecording(roomName, fileName);
      if (egress?.egressId) {
        const update = {
          egressId: egress.egressId,
          recordingStatus: 'recording',
          recordingError: ''
        };
        // Best-effort S3 URL; the completion webhook remains authoritative.
        if (egress.recordingUrl) update.recordingUrl = egress.recordingUrl;
        await Session.updateOne(
          { _id: claimed._id, recordingStatus: 'starting' },
          { $set: update }
        );
      } else {
        const message = 'LiveKit did not return an egress ID';
        console.error('Automatic session recording failed', {
          sessionId: String(claimed._id),
          type: claimed.type,
          roomName,
          message
        });
        await Session.updateOne(
          { _id: claimed._id, recordingStatus: 'starting' },
          { $set: { recordingStatus: 'failed', recordingError: message } }
        );
      }
    }
    session = await Session.findById(session._id);
  }

  await createAndBroadcastNotification(req, {
    recipient: session.user,
    type: 'session_started',
    title: 'Session started',
    body: 'Your advisor has started the session',
    data: { sessionId: session._id }
  }, 'session:started');

  return sendResponse(res, { message: 'Session live', data: session });
});

// ============= End session =============
export const endSession = catchAsync(async (req, res) => {
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (
    String(session.user) !== String(req.user._id) &&
    String(session.advisor) !== String(req.user._id)
  ) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');

  if (session.status !== 'live') throw new ApiError(StatusCodes.BAD_REQUEST, 'Session is not live');

  session.endedAt = new Date();
  if (session.egressId) {
    const egressInfo = await stopEgress(session.egressId);
    if (egressInfo) {
      const recordingUrl = await resolveRecordingUrl(egressInfo);
      if (recordingUrl) {
        session.recordingUrl = recordingUrl;
        session.recordingStatus = 'completed';
        session.recordingError = '';
      }
    } else if (session.recordingUrl && session.recordingStatus === 'recording') {
      session.recordingStatus = 'completed';
    }
  }

  await settleSession(session);
  await session.save();

  // tear down room
  if (session.livekitRoom) await deleteRoom(session.livekitRoom);

  await createAndBroadcastNotification(req, {
    recipient: session.user,
    type: 'session_completed',
    title: 'Session completed',
    body: 'Your session has ended. Leave a review?',
    data: { sessionId: session._id }
  }, 'session:ended');
  await createAndBroadcastNotification(req, {
    recipient: session.advisor,
    type: 'session_completed',
    title: 'Session completed',
    body: 'Session ended successfully',
    data: { sessionId: session._id }
  }, 'session:ended');

  return sendResponse(res, { message: 'Session ended', data: session });
});

// ============= Heartbeat / per-minute charge =============
export const sessionHeartbeat = catchAsync(async (req, res) => {
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (
    String(session.user) !== String(req.user._id) &&
    String(session.advisor) !== String(req.user._id)
  ) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');
  if (session.status !== 'live') return sendResponse(res, { data: { ended: true, session } });

  const elapsedSec = Math.max(0, Math.round((Date.now() - new Date(session.startedAt).getTime()) / 1000));
  const elapsedMin = elapsedSec / 60;
  const targetCharge = Math.ceil(elapsedMin * session.ratePerMin);
  const diff = round2(targetCharge - (session.chargedAmount || 0));

  let lowBalanceWarning = false;
  let autoEnded = false;

  if (diff > 0) {
    try {
      const { creditsUsed, balanceUsed } = await chargeUserWallet({ userId: session.user, amount: diff });
      session.creditsUsed = round2((session.creditsUsed || 0) + creditsUsed);
      session.chargedAmount = round2((session.chargedAmount || 0) + creditsUsed + balanceUsed);
      await Transaction.create({
        type: 'session_charge',
        status: 'completed',
        user: session.user,
        advisor: session.advisor,
        session: session._id,
        amount: round2(creditsUsed + balanceUsed),
        description: `Per-minute charge for ${session.sessionCode}`
      });
    } catch (e) {
      // insufficient balance: end session
      session.endedAt = new Date();
      if (session.egressId) await stopEgress(session.egressId);
      await settleSession(session);
      autoEnded = true;
      await createAndBroadcastNotification(req, {
        recipient: session.user,
        type: 'low_balance',
        title: 'Session ended — low balance',
        body: 'Your wallet ran out of funds. Add funds to continue next time.',
        data: { sessionId: session._id }
      }, 'session:ended');
    }
  }

  // Low balance warning if remaining < threshold
  const wallet = await Wallet.findOne({ user: session.user }).lean();
  const remainingMins = ((wallet?.balance || 0) + (wallet?.freeCredits || 0)) / session.ratePerMin;
  if (!autoEnded && remainingMins < (Number(process.env.SESSION_LOW_BALANCE_THRESHOLD_MIN) || 2)) {
    lowBalanceWarning = true;
  }

  await session.save();

  return sendResponse(res, {
    data: {
      session,
      elapsedSec,
      remainingMins: Math.max(0, remainingMins),
      lowBalanceWarning,
      autoEnded
    }
  });
});

// ============= Extend session =============
export const extendSession = catchAsync(async (req, res) => {
  const { minutes } = req.body; // 5|10|15
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (String(session.user) !== String(req.user._id)) throw new ApiError(StatusCodes.FORBIDDEN, 'Only the user can extend');
  if (session.status !== 'live') throw new ApiError(StatusCodes.BAD_REQUEST, 'Session is not live');

  const cost = Math.ceil((Number(minutes) || 0) * session.ratePerMin);
  if (cost <= 0) throw new ApiError(StatusCodes.BAD_REQUEST, 'Invalid minutes');

  // verify availability of funds
  const wallet = await Wallet.findOne({ user: session.user });
  if ((wallet?.balance || 0) + (wallet?.freeCredits || 0) < cost) {
    throw new ApiError(StatusCodes.PAYMENT_REQUIRED, 'Insufficient balance to extend');
  }

  session.durationMinutes = (session.durationMinutes || 0) + Number(minutes);
  session.extensions.push({ minutes: Number(minutes), cost });
  await session.save();

  await createAndBroadcastNotification(req, {
    recipient: session.advisor,
    type: 'session_updated',
    title: 'Session updated',
    body: `Session extended by ${Number(minutes)} minutes`,
    data: { sessionId: session._id }
  }, 'session:updated');

  return sendResponse(res, { message: 'Session extended', data: session });
});

// ============= Cancel session (user or advisor) =============
export const cancelSession = catchAsync(async (req, res) => {
  const { reason } = req.body;
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');

  const isUser = String(session.user) === String(req.user._id);
  const isAdvisor = String(session.advisor) === String(req.user._id);
  if (!isUser && !isAdvisor) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');
  if (!['pending', 'consent', 'waiting'].includes(session.status)) {
    throw new ApiError(StatusCodes.BAD_REQUEST, `Cannot cancel session in status ${session.status}`);
  }

  // refund any held charge
  if ((session.chargedAmount || 0) > 0) {
    await refundToUserWallet({ userId: session.user, amount: session.chargedAmount });
    await Transaction.create({
      type: 'session_refund',
      status: 'completed',
      user: session.user,
      session: session._id,
      amount: session.chargedAmount,
      description: `Refund for cancelled session ${session.sessionCode}`
    });
    session.refundIssued = round2((session.refundIssued || 0) + session.chargedAmount);
    session.chargedAmount = 0;
  }

  session.status = 'cancelled';
  session.cancelledBy = req.user._id;
  session.cancelReason = reason || '';
  session.cancelledAt = new Date();
  await session.save();
  await releaseSlotLocks(session._id);

  if (session.livekitRoom) await deleteRoom(session.livekitRoom);

  // Update advisor stats (cancelled count)
  if (isAdvisor) {
    await AdvisorProfile.findOneAndUpdate({ user: session.advisor }, { $inc: { cancelledSessions: 1 } });
  }

  await createAndBroadcastNotification(req, {
    recipient: isUser ? session.advisor : session.user,
    type: 'session_cancelled',
    title: 'Session cancelled',
    body: `Session was cancelled${reason ? ': ' + reason : ''}`,
    data: { sessionId: session._id }
  }, 'session:cancelled');

  return sendResponse(res, { message: 'Session cancelled and any holds refunded', data: session });
});

// ============= Reschedule session =============
export const rescheduleSession = catchAsync(async (req, res) => {
  const { newScheduledFor, reason } = req.body;
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (
    String(session.user) !== String(req.user._id) &&
    String(session.advisor) !== String(req.user._id)
  ) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');

  if (!['pending', 'consent', 'waiting'].includes(session.status)) {
    throw new ApiError(StatusCodes.BAD_REQUEST, `Cannot reschedule session in status ${session.status}`);
  }

  const profile = await AdvisorProfile.findOne({ user: session.advisor }).populate('user', 'timezone country');
  if (!profile) throw new ApiError(StatusCodes.NOT_FOUND, 'Advisor profile missing');
  const nextStart = new Date(newScheduledFor);
  await assertAdvisorSlotAvailable({
    advisorId: session.advisor,
    profile,
    start: nextStart,
    durationMinutes: session.durationMinutes,
    excludeSessionId: session._id
  });

  const previousStart = session.scheduledFor;
  await releaseSlotLocks(session._id);
  try {
    await reserveSlotLocks({
      advisorId: session.advisor,
      sessionId: session._id,
      start: nextStart,
      durationMinutes: session.durationMinutes
    });
  } catch (error) {
    if (previousStart) {
      await reserveSlotLocks({
        advisorId: session.advisor,
        sessionId: session._id,
        start: previousStart,
        durationMinutes: session.durationMinutes
      });
    }
    throw error;
  }

  session.rescheduledFrom = session.scheduledFor;
  session.scheduledFor = nextStart;
  session.rescheduleReason = reason || '';
  session.rescheduledAt = new Date();
  await session.save();

  await createAndBroadcastNotification(req, {
    recipient: String(req.user._id) === String(session.user) ? session.advisor : session.user,
    type: 'session_rescheduled',
    title: 'Session rescheduled',
    body: `Session moved to ${session.scheduledFor.toISOString()}`,
    data: { sessionId: session._id }
  }, 'session:rescheduled');

  return sendResponse(res, { message: 'Session rescheduled', data: session });
});

// ============= Tip advisor =============
export const tipAdvisor = catchAsync(async (req, res) => {
  const { amount } = req.body;
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (String(session.user) !== String(req.user._id)) throw new ApiError(StatusCodes.FORBIDDEN, 'Only the user can tip');
  if (!amount || Number(amount) <= 0) throw new ApiError(StatusCodes.BAD_REQUEST, 'Invalid amount');

  const amt = round2(Number(amount));
  await chargeUserWallet({ userId: session.user, amount: amt });
  await Wallet.findOneAndUpdate({ user: session.advisor }, { $inc: { earningsBalance: amt, totalEarned: amt } }, { upsert: true });

  await Transaction.create({
    type: 'tip',
    status: 'completed',
    user: session.user,
    advisor: session.advisor,
    session: session._id,
    amount: amt,
    description: `Tip for session ${session.sessionCode}`
  });
  await Transaction.create({
    type: 'advisor_tip',
    status: 'completed',
    user: session.user,
    advisor: session.advisor,
    session: session._id,
    amount: amt,
    description: `Tip received from session ${session.sessionCode}`
  });

  session.tipAmount = round2((session.tipAmount || 0) + amt);
  await session.save();

  await createAndBroadcastNotification(req, {
    recipient: session.advisor,
    type: 'tip_received',
    title: 'You received a tip',
    body: `${amt} credits tip from your client`,
    data: { sessionId: session._id, amount: amt }
  }, 'session:updated');

  return sendResponse(res, { message: 'Tip sent', data: session });
});

export const tipAdvisorWithIap = catchAsync(async (req, res) => {
  const result = await recordIapTip({
    userId: req.user._id,
    sessionId: req.params.id,
    body: req.body || {}
  });

  if (result.duplicate) {
    return sendResponse(res, {
      message: 'Tip already recorded',
      data: { transaction: result.transaction, session: result.session }
    });
  }

  await createAndBroadcastNotification(req, {
    recipient: result.session.advisor,
    type: 'tip_received',
    title: 'You received a tip',
    body: `You received a ${String(result.transaction.currency || 'usd').toUpperCase()} ${Number(result.transaction.amount || 0).toFixed(2)} tip from your client`,
    data: {
      sessionId: result.session._id,
      transactionId: result.transaction._id,
      amountUsd: result.transaction.amountUsd,
      currency: result.transaction.currency,
      amount: result.transaction.amount,
      payoutCredits: result.payoutCredits
    }
  }, 'session:updated');

  return sendResponse(res, {
    message: 'Tip sent',
    data: {
      transaction: result.transaction,
      advisorTransaction: result.advisorTransaction,
      session: result.session
    }
  });
});

// ============= Unlock recording / transcript =============
export const unlockSessionAsset = catchAsync(async (req, res) => {
  const { asset } = req.body; // 'recording' | 'transcript'
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (String(session.user) !== String(req.user._id)) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');
  if (!SESSION_ASSET_STATUSES.includes(session.status)) {
    throw new ApiError(StatusCodes.CONFLICT, 'Session assets can be unlocked after the session ends');
  }

  if (asset === 'recording') {
    if (!['call', 'video'].includes(session.type)) {
      throw new ApiError(StatusCodes.BAD_REQUEST, 'Text sessions do not have media recordings');
    }
    if (session.recordingPriceUnlocked) {
      return sendResponse(res, { message: 'recording already unlocked', data: session });
    }
    if (
      !session.recordingUrl ||
      ['starting', 'recording', 'failed'].includes(session.recordingStatus)
    ) {
      throw new ApiError(StatusCodes.CONFLICT, 'Recording is not available yet');
    }
  } else if (asset === 'transcript') {
    if (session.type !== 'chat') {
      throw new ApiError(StatusCodes.BAD_REQUEST, 'Only text sessions have chat transcripts');
    }
    if (session.transcriptPriceUnlocked) {
      return sendResponse(res, { message: 'transcript already unlocked', data: session });
    }
  }

  let amount = 0;
  const usage = await getCreditUsage();
  if (asset === 'recording') {
    const defaultRecordingCharge = session.type === 'video'
      ? usage.videoRecording ?? usage.sessionRecording
      : usage.audioRecording ?? usage.sessionRecording;
    amount = session.unlockChargeRecording || defaultRecordingCharge;
  }
  else if (asset === 'transcript') amount = session.unlockChargeTranscript || usage.chatTranscript;
  else throw new ApiError(StatusCodes.BAD_REQUEST, 'Invalid asset');

  await chargeUserWallet({ userId: session.user, amount });
  await Transaction.create({
    type: asset === 'recording' ? 'unlock_recording' : 'unlock_transcript',
    status: 'completed',
    user: session.user,
    session: session._id,
    amount,
    description: `Unlock ${asset} for session ${session.sessionCode}`
  });

  if (asset === 'recording') session.recordingPriceUnlocked = true;
  else session.transcriptPriceUnlocked = true;

  await session.save();
  return sendResponse(res, { message: `${asset} unlocked`, data: session });
});

// ============= Ongoing session for user =============
export const getOngoing = catchAsync(async (req, res) => {
  const isAdvisor = req.user.role === 'advisor';
  const filter = isAdvisor ? { advisor: req.user._id, status: 'live' } : { user: req.user._id, status: 'live' };
  const sessionDoc = await Session.findOne(filter)
    .populate('user', 'name profilePhoto')
    .populate('advisor', 'name profilePhoto');
  const session = await reconcileTimedOutSession(sessionDoc);
  if (session?.status !== 'live') {
    return sendResponse(res, { data: null });
  }
  return sendResponse(res, {
    data: isAdvisor ? session.toObject() : sessionForUserResponse(session)
  });
});

// ============= Session complete summary (for "Session Completed" modal) =============
export const sessionSummary = catchAsync(async (req, res) => {
  const sessionDoc = await Session.findById(req.params.id)
    .populate('user', 'name profilePhoto')
    .populate('advisor', 'name profilePhoto');
  if (!sessionDoc) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  const session = await reconcileTimedOutSession(sessionDoc);

  const review = await Review.findOne({ session: session._id }).lean();
  return sendResponse(res, { data: { session: session.toObject(), review } });
});

// ============= Save advisor session note =============
export const saveSessionNote = catchAsync(async (req, res) => {
  // Notes are stored as a quick metadata field (no separate model needed)
  const { notes } = req.body;
  const session = await Session.findById(req.params.id);
  if (!session) throw new ApiError(StatusCodes.NOT_FOUND, 'Session not found');
  if (String(session.advisor) !== String(req.user._id)) throw new ApiError(StatusCodes.FORBIDDEN, 'Forbidden');
  session.set('advisorNotes', notes || '');
  await session.save();
  return sendResponse(res, { data: session });
});
