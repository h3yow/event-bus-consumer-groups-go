// Package bus implements a local, durable, event-driven message bus with
// consumer groups, per-partition ordered delivery, retries, dead-lettering,
// offset-based recovery after process restart, and a unified bounded
// graceful shutdown (Bus.Shutdown) that drains in-flight work, fences late
// commits, persists an explainable per-group/per-partition report, and
// guarantees uncommitted messages are redelivered on the next start.
package bus
