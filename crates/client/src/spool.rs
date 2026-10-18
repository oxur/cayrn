//! Bounded client presentation storage, separate from the authoritative journal.
use cayrn_protocol::{Cursor, ErrorCode, Event, Fault, MAX_FRAME, Page, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

/// A temporary disk spool with atomic page acceptance and contiguous cursors.
/// It never executes code or acknowledges processing to the server.
#[derive(Debug)]
pub struct EventSpool {
    file: File,
    session: String,
    received: Cursor,
    displayed: Cursor,
    read: u64,
    bytes: u64,
    capacity: u64,
}
impl EventSpool {
    /// Allocate private temporary storage; capacity bounds total pending file bytes.
    pub fn new(session: &str, after: Cursor, capacity: u64) -> Result<Self> {
        cayrn_protocol::validate_id(session)?;
        Ok(Self {
            file: tempfile::tempfile()?,
            session: session.into(),
            received: after,
            displayed: after,
            read: 0,
            bytes: 0,
            capacity,
        })
    }
    /// Last contiguous event safely accepted into this presentation spool.
    pub fn received(&self) -> Cursor {
        self.received
    }
    /// Last event removed for presentation; persist only after actual rendering succeeds.
    pub fn displayed(&self) -> Cursor {
        self.displayed
    }
    /// Accept a complete page or retain the previous cursor on error.
    pub fn append(&mut self, page: &Page) -> Result<()> {
        let mut expected = self.received.0;
        let mut bytes = Vec::new();
        for event in &page.events {
            expected = expected
                .checked_add(1)
                .ok_or_else(|| Fault::new(ErrorCode::Protocol, "cursor exhausted"))?;
            if event.session != self.session || event.sequence.0 != expected {
                return Err(Fault::new(
                    ErrorCode::Protocol,
                    "event page is not the contiguous session stream",
                ));
            }
            let payload = serde_json::to_vec(event)
                .map_err(|e| Fault::new(ErrorCode::Protocol, e.to_string()))?;
            if payload.len() > MAX_FRAME {
                return Err(Fault::new(
                    ErrorCode::Protocol,
                    "presentation event exceeds frame limit",
                ));
            }
            bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&payload);
            if bytes.len() > cayrn_protocol::PAGE_BYTES + 4 * usize::from(cayrn_protocol::MAX_PAGE)
            {
                return Err(Fault::new(
                    ErrorCode::Protocol,
                    "presentation page exceeds budget",
                ));
            }
        }
        if page.next.0 != expected || page.head.0 < expected {
            return Err(Fault::new(
                ErrorCode::Protocol,
                "event page cursor mismatch",
            ));
        }
        if self.bytes + bytes.len() as u64 > self.capacity {
            return Err(Fault::new(
                ErrorCode::Busy,
                format!(
                    "presentation spool full; resume after displayed cursor {}",
                    self.displayed.0
                ),
            ));
        }
        self.file.seek(SeekFrom::Start(self.bytes))?;
        if let Err(error) = self
            .file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_data())
        {
            if let Err(rollback) = self.file.set_len(self.bytes) {
                return Err(Fault::new(
                    ErrorCode::Io,
                    format!("spool write failed: {error}; rollback failed: {rollback}"),
                ));
            }
            return Err(error.into());
        }
        self.bytes += bytes.len() as u64;
        self.received = page.next;
        Ok(())
    }
    /// Remove one bounded event for display. No terminal I/O occurs inside the spool.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        if self.read == self.bytes {
            return Ok(None);
        }
        self.file.seek(SeekFrom::Start(self.read))?;
        let mut size = [0; 4];
        self.file.read_exact(&mut size)?;
        let size = u32::from_be_bytes(size) as usize;
        if size > MAX_FRAME {
            return Err(Fault::new(
                ErrorCode::Protocol,
                "corrupt presentation spool",
            ));
        }
        let mut payload = vec![0; size];
        self.file.read_exact(&mut payload)?;
        let event: Event = serde_json::from_slice(&payload)
            .map_err(|e| Fault::new(ErrorCode::Protocol, e.to_string()))?;
        self.read += 4 + size as u64;
        self.displayed = event.sequence;
        if self.read == self.bytes {
            self.file.set_len(0)?;
            self.read = 0;
            self.bytes = 0;
        }
        Ok(Some(event))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use cayrn_protocol::EventKind;
    fn page(sequence: u64) -> Page {
        Page {
            events: vec![Event {
                session: "session".into(),
                generation: "generation".into(),
                sequence: Cursor(sequence),
                event: EventKind::SessionCreated,
            }],
            next: Cursor(sequence),
            head: Cursor(sequence),
        }
    }
    #[test]
    fn capacity_and_order_errors_preserve_resume_cursor() {
        let mut spool = EventSpool::new("session", Cursor(0), 1024).unwrap();
        spool.append(&page(1)).unwrap();
        assert!(spool.append(&page(1)).is_err());
        assert!(spool.append(&page(3)).is_err());
        assert_eq!(spool.received(), Cursor(1));
        assert_eq!(spool.next_event().unwrap().unwrap().sequence, Cursor(1));
        spool.append(&page(2)).unwrap();
        assert_eq!(spool.next_event().unwrap().unwrap().sequence, Cursor(2));
        assert!(spool.next_event().unwrap().is_none());
        let mut full = EventSpool::new("session", Cursor(0), 1).unwrap();
        assert!(full.append(&page(1)).is_err());
        assert_eq!(full.received(), Cursor(0));
        assert!(full.next_event().unwrap().is_none());
    }
    #[test]
    fn wrong_session_and_cursor_cannot_be_accepted() {
        let mut spool = EventSpool::new("session", Cursor(0), 1024).unwrap();
        let mut wrong = page(1);
        wrong.events[0].session = "other".into();
        assert!(spool.append(&wrong).is_err());
        let mut wrong = page(1);
        wrong.next = Cursor(9);
        assert!(spool.append(&wrong).is_err());
        assert_eq!(spool.received(), Cursor(0));
    }
}
