//! The in-process consumer surface for one authoritative book: latest state through a
//! coalescing watch channel, and mutation history through one bounded ring.
//!
//! The two surfaces are separate because they have different natures. Latest state is
//! coalescable — a consumer that wants only what is true now loses nothing by skipping
//! intermediate revisions — so it is published by pointer swap and can never overflow.
//! Mutation history is not coalescable, so it travels through a bounded drop-oldest ring
//! whose overflow is reported as an explicit continuity loss. Keeping them apart is what
//! makes a burst of mutation-free revisions unable to cost a consumer its continuity.
//!
//! A consumer never blocks the writer and never holds a lock the writer needs to make
//! progress.

use crate::{
    AuthorityReason, BookCommit, BookError, BookMutation, Candidate, ConsumerState,
    ContinuityReason, MarketResolution, MutationContinuity, MutationCursor, OrderBook,
    PublishedBook,
};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

const MAX_OBSERVER_CAPACITY: usize = 1 << 20;

/// The fixed depth of one book's mutation ring, in deliveries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObserverCapacity(usize);
impl ObserverCapacity {
    /// Rejects zero and anything above 1048576 deliveries, so the ring is always bounded
    /// and always able to hold at least one delivery.
    pub fn new(value: usize) -> Result<Self, BookError> {
        if value == 0 || value > MAX_OBSERVER_CAPACITY {
            Err(BookError::CapacityOutOfRange)
        } else {
            Ok(Self(value))
        }
    }
    pub fn value(self) -> usize {
        self.0
    }
}

/// Whether a ring overrun cost an attachment anything its own state did not already
/// contain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverrunVerdict {
    Harmless,
    Lost,
}

/// A ring overrun observed but not yet judged against an attachment boundary.
///
/// An overrun cannot be judged when it is reported: the ring names only how many
/// deliveries it dropped, and which positions those were is known only once the ring
/// resumes. Consecutive overruns before a resuming delivery are one unresolved span, so
/// `missed` accumulates, saturating rather than wrapping.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PendingOverrun {
    missed: u64,
}
impl PendingOverrun {
    pub fn new(missed: u64) -> Self {
        Self { missed }
    }
    /// The total the ring has reported dropped across this unresolved span.
    pub fn missed(self) -> u64 {
        self.missed
    }
    pub fn accumulated(self, missed: u64) -> Self {
        Self {
            missed: self.missed.saturating_add(missed),
        }
    }
}

/// Judges a ring overrun against the boundary the attachment started from.
///
/// `boundary` is the first stream position the attachment's own state does not contain;
/// `resumed` is the cursor of the first delivery the ring produced after the overrun, or
/// `None` when the ring closed without producing one. The dropped deliveries are exactly
/// the positions immediately preceding `resumed`, so all of them precede the boundary —
/// and are therefore already contained in the state the attachment read — exactly when
/// `resumed` carries the boundary's epoch and sits at or below it. A resuming position
/// above the boundary, an epoch change across the gap, and a close with nothing to resume
/// from are all unprovable and are reported as [`OverrunVerdict::Lost`].
pub fn classify_overrun(
    boundary: &MutationCursor,
    resumed: Option<&MutationCursor>,
) -> OverrunVerdict {
    match resumed {
        Some(cursor)
            if cursor.epoch() == boundary.epoch() && cursor.position() <= boundary.position() =>
        {
            OverrunVerdict::Harmless
        }
        _ => OverrunVerdict::Lost,
    }
}

/// One level change with its position in the book's mutation stream.
///
/// Latest-state notifications travel the separate watch surface and never consume ring
/// capacity. A delivery is source-reported or locally derived according to
/// `mutation().provenance().origin()`; both travel the one ring [`StreamDelivery`] names,
/// in the single order the writer committed them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationDelivery {
    revision: u64,
    cursor: MutationCursor,
    mutation: Arc<BookMutation>,
}
impl MutationDelivery {
    /// The book revision whose commit produced this mutation.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn cursor(&self) -> &MutationCursor {
        &self.cursor
    }
    pub fn mutation(&self) -> &Arc<BookMutation> {
        &self.mutation
    }
}

/// One venue-reported resolution with its position in the book's delivery stream.
///
/// The position is drawn from the book's own [`OrderBook::note_stream_event`] counter, so a
/// resolution is ordered against the level changes around it and no later commit can reuse
/// its position. `revision` is the book revision the resolution is ordered *after*: a
/// resolution derives no level change, so it advances no revision and the book it names is
/// the one that was current when it arrived. Lifecycle and book state stay independent — a
/// delivered resolution says what the venue reported, never that the book stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionDelivery {
    revision: u64,
    cursor: MutationCursor,
    resolution: Arc<MarketResolution>,
}
impl ResolutionDelivery {
    /// The book revision this resolution is ordered after. Unchanged by the resolution.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn cursor(&self) -> &MutationCursor {
        &self.cursor
    }
    pub fn resolution(&self) -> &Arc<MarketResolution> {
        &self.resolution
    }
}

/// Everything one book's single ordered delivery lane carries.
///
/// Both kinds travel the one bounded ring, in the single order the writer produced them, so
/// a consumer never has to reconcile two lanes against each other: a resolution delivered
/// between two mutations really did arrive between them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamDelivery {
    Mutation(MutationDelivery),
    Resolution(ResolutionDelivery),
}
impl StreamDelivery {
    /// This delivery's position in the book's stream, whichever kind it is.
    pub fn cursor(&self) -> &MutationCursor {
        match self {
            Self::Mutation(delivery) => delivery.cursor(),
            Self::Resolution(delivery) => delivery.cursor(),
        }
    }
    /// The book revision this delivery belongs to: the one a mutation's commit produced, or
    /// the one a resolution is ordered after.
    pub fn revision(&self) -> u64 {
        match self {
            Self::Mutation(delivery) => delivery.revision(),
            Self::Resolution(delivery) => delivery.revision(),
        }
    }
}

/// What a combined wait on both of a consumer's surfaces produced.
///
/// [`Self::Published`] is a coalescing latest-state notification: it says a newer revision
/// exists and hands over that state, not every revision in between.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserverEvent {
    Published(Arc<PublishedBook>),
    Mutation(MutationDelivery),
    Resolution(ResolutionDelivery),
}
impl From<StreamDelivery> for ObserverEvent {
    fn from(delivery: StreamDelivery) -> Self {
        match delivery {
            StreamDelivery::Mutation(delivery) => Self::Mutation(delivery),
            StreamDelivery::Resolution(delivery) => Self::Resolution(delivery),
        }
    }
}

/// Why a mutation receive produced no delivery.
///
/// [`Self::ContinuityLost`] is raised for two facts, told apart by `reason`.
///
/// [`ContinuityReason::RecoveryBase`] is the rebased-stream fact: the writer opened a new
/// continuity epoch — a recovery base after a reported loss, or a divergence checkpoint —
/// and the wholesale state replacement that came with it was published as state, never as
/// mutations. An attachment applying mutations incrementally would silently skip it, so the
/// attachment is failed instead. `missed` of zero is meaningful and usual: no ring drop took
/// part, the loss is the rebase itself. A nonzero `missed` says the ring had also dropped
/// that many deliveries when the rebase was found; the rebase is still the reason, because
/// restarting the attachment is what answers both.
///
/// [`ContinuityReason::Overrun`] is the overtaken-consumer result. It is raised only for an
/// overrun [`classify_overrun`] could not rule harmless: dropping deliveries the attached
/// state already contains costs the consumer nothing and is not a loss. `missed` totals
/// what the ring reported dropped across the unresolved span — every consecutive overrun
/// since the last delivery, not just the last of them — and is repeated unchanged while
/// the loss stands, so a writer that keeps publishing during that window drops more than
/// the reported figure. The count is evidence that history broke, not a quantity to
/// reconcile against; the only correct response is [`BookObserver::reattach`] whatever the
/// number. The loss is never silently swallowed and never presented as an empty ring, and
/// it persists across every receive until that reattach. A writer that goes away while an
/// overrun is still unresolved yields that loss rather than [`Self::Closed`], because a
/// gap that can no longer be examined is not evidence of harmlessness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserverRecvError {
    ContinuityLost {
        reason: ContinuityReason,
        missed: u64,
    },
    Closed,
}
impl std::fmt::Display for ObserverRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no mutation delivery")
    }
}
impl std::error::Error for ObserverRecvError {}

/// The writer is gone, so no revision after the one [`BookObserver::latest`] reads will
/// ever be published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterGone;
impl std::fmt::Display for WriterGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the book writer is gone")
    }
}
impl std::error::Error for WriterGone {}

/// The one writer for a book and its consumer surface.
///
/// It owns the only strong sender of each surface, so dropping it closes both: a consumer
/// learns the writer is gone rather than waiting forever. Applying a candidate publishes the
/// new state before sending its mutations, so a consumer woken by a mutation for revision R
/// always finds at least revision R when it reads state.
#[derive(Debug)]
pub struct BookWriter {
    book: OrderBook,
    state: watch::Sender<Arc<PublishedBook>>,
    deliveries: broadcast::Sender<StreamDelivery>,
}

impl BookWriter {
    /// Takes ownership of `book` as its single writer and publishes its current state.
    pub fn new(book: OrderBook, capacity: ObserverCapacity) -> Self {
        let (deliveries, _) = broadcast::channel(capacity.value());
        let (state, _) = watch::channel(Arc::new(book.publish()));
        Self {
            book,
            state,
            deliveries,
        }
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    /// The newest published state. The watch borrow guard is held only for the clone.
    pub fn published(&self) -> Arc<PublishedBook> {
        Arc::clone(&self.state.borrow())
    }

    /// Attaches a consumer to a coherent state revision and ring position as one operation.
    ///
    /// The ring subscription is taken before the state is read, so no mutation sent after
    /// the read can be missed; the observer then suppresses mutations at or below the
    /// revision it read, so nothing already contained in that state is replayed. The new
    /// watch receiver starts with the current revision marked seen, so a fresh observer is
    /// not immediately told about state it just read. The attachment also records the
    /// boundary that state stops at, so a later ring overrun can be judged against it.
    pub fn attach(&self) -> BookObserver {
        let deliveries = self.deliveries.subscribe();
        let state = self.state.subscribe();
        let published = Arc::clone(&state.borrow());
        let boundary = attachment_cursor(&published);
        BookObserver {
            state,
            deliveries,
            resume_after: published.revision(),
            cursor: boundary.clone(),
            boundary,
            continuous: true,
            pending: None,
            lost: None,
            state_open: true,
            closed: false,
        }
    }

    /// Applies a snapshot, publishes the resulting revision, then sends its mutations.
    ///
    /// Neither publication nor sending can block or fail the apply: the state surface
    /// coalesces onto the newest value, and a full ring drops its oldest mutation. Failures
    /// are exactly [`OrderBook::apply_snapshot`]'s, and leave both book and published state
    /// unchanged.
    ///
    /// Publishing before sending means an attachment taken between the two reads a state
    /// that already contains mutations still to reach the ring. Those deliveries are
    /// redundant for it, so dropping them costs it nothing: they fall before its recorded
    /// boundary and [`classify_overrun`] rules the overrun harmless.
    pub fn apply_snapshot(&mut self, candidate: &Candidate) -> Result<BookCommit, BookError> {
        let commit = self.book.apply_snapshot(candidate)?;
        Ok(self.publish_then_send(commit))
    }

    /// Applies a source delta, publishes the resulting revision, then sends its mutations.
    ///
    /// Ordering, blocking, and overflow behave exactly as [`Self::apply_snapshot`]: state
    /// first, mutations after, neither able to block or fail the apply. Failures are exactly
    /// [`OrderBook::apply_source_delta`]'s, and leave both book and published state
    /// unchanged, so a rejected delta never reaches the ring.
    pub fn apply_source_delta(&mut self, candidate: &Candidate) -> Result<BookCommit, BookError> {
        let commit = self.book.apply_source_delta(candidate)?;
        Ok(self.publish_then_send(commit))
    }

    fn publish_then_send(&mut self, commit: BookCommit) -> BookCommit {
        self.state.send_replace(Arc::new(self.book.publish()));
        for record in commit.mutations() {
            let _ = self
                .deliveries
                .send(StreamDelivery::Mutation(MutationDelivery {
                    revision: commit.revision(),
                    cursor: record.cursor().clone(),
                    mutation: Arc::clone(record.mutation()),
                }));
        }
        commit
    }

    /// Publishes the state naming the position past one venue-reported resolution, then
    /// sends that resolution on this book's delivery lane.
    ///
    /// The position comes from [`OrderBook::note_stream_event`], so no later commit can
    /// reuse it and no consumer can be handed a silently overwritten slot. The book itself
    /// does not move: revision, levels, authority and provenance are unchanged, the market
    /// stays subscribed, and a book update arriving after a resolution is applied exactly as
    /// one arriving before it.
    ///
    /// Publishing before sending is [`Self::apply_snapshot`]'s order and is required for the
    /// same reason. [`Self::attach`] subscribes to the ring before it reads state, so an
    /// attachment taken between the two reads a state whose boundary is already past the
    /// delivery: the delivery it then receives — or, having subscribed at the ring's tail,
    /// never receives — is judged redundant against that boundary, which is the honest
    /// late-attacher semantics. Sending first would let that attachment record the boundary
    /// *before* the resolution, never see the resolution already sent, and then admit the
    /// next delivery as if nothing were missing: a silently skipped resolution with no loss
    /// reported.
    ///
    /// Fails with [`BookError::ContinuityLost`] when the stream is lost — a lost stream has
    /// no positions, and its consumers are already under an explicit continuity loss — in
    /// which case nothing is published and nothing is sent. Neither publishing nor sending
    /// can block or fail the caller: the state surface coalesces, and a full ring drops its
    /// oldest delivery exactly as it does for a mutation.
    pub fn publish_resolution(
        &mut self,
        resolution: Arc<MarketResolution>,
    ) -> Result<ResolutionDelivery, BookError> {
        let cursor = self.book.note_stream_event()?;
        let delivery = ResolutionDelivery {
            revision: self.book.revision(),
            cursor,
            resolution,
        };
        self.state.send_replace(Arc::new(self.book.publish()));
        let _ = self
            .deliveries
            .send(StreamDelivery::Resolution(delivery.clone()));
        Ok(delivery)
    }

    /// Publishes this book's final state: no longer subscribed.
    ///
    /// Reports whether anything changed. The state surface is updated so every reader that
    /// holds this book — including one that only ever calls
    /// [`BookObserver::latest`] and never reads a mutation — observes
    /// [`AuthorityState::Unsubscribed`] before the writer goes away. Dropping the writer
    /// alone would leave such a reader holding a state that still said `Live` forever.
    pub fn unsubscribe(&mut self) -> Result<bool, BookError> {
        let changed = self.book.unsubscribe()?;
        if changed {
            self.state.send_replace(Arc::new(self.book.publish()));
        }
        Ok(changed)
    }

    /// Reports an evidence-based loss to the book and publishes it when state changed.
    ///
    /// A loss derives no mutation, so nothing reaches the ring; consumers learn of it by
    /// reading the published state, which carries the stale authority and the lost
    /// continuity. A repeated identical report publishes nothing.
    pub fn report_continuity_loss(
        &mut self,
        continuity: ContinuityReason,
        authority: AuthorityReason,
    ) -> Result<bool, BookError> {
        let changed = self.book.report_continuity_loss(continuity, authority)?;
        if changed {
            self.state.send_replace(Arc::new(self.book.publish()));
        }
        Ok(changed)
    }
}

/// One consumer's attachment: a coalescing latest-state reader plus an independent ring
/// cursor over the mutation history.
///
/// Reads never touch the writer's book, and a slow read never delays the writer. The
/// observer holds only receivers, so it cannot keep either surface open past the writer.
#[derive(Debug)]
pub struct BookObserver {
    state: watch::Receiver<Arc<PublishedBook>>,
    deliveries: broadcast::Receiver<StreamDelivery>,
    resume_after: u64,
    boundary: MutationCursor,
    cursor: MutationCursor,
    continuous: bool,
    pending: Option<PendingOverrun>,
    lost: Option<ObserverRecvError>,
    state_open: bool,
    closed: bool,
}

enum Waited {
    State(Result<(), watch::error::RecvError>),
    Delivery(Result<StreamDelivery, broadcast::error::RecvError>),
}

impl BookObserver {
    /// Reads the newest published book without disturbing this attachment.
    ///
    /// The watch borrow guard is held only long enough to clone the pointer, the same
    /// momentary-reader class as the writer's own publication: no consumer work happens
    /// under it, so a slow consumer cannot delay a publish.
    pub fn latest(&self) -> Arc<PublishedBook> {
        Arc::clone(&self.state.borrow())
    }

    /// Waits until a revision newer than the one this observer last read is published, and
    /// returns it.
    ///
    /// This surface coalesces: several revisions published while the consumer was away
    /// resolve to one wake carrying the newest of them. It can never report a continuity
    /// loss, because skipping intermediate latest states loses nothing. Fails with
    /// [`WriterGone`] once the writer is dropped and every revision it published has been
    /// reported, so a revision is never lost to the writer going away; [`Self::latest`]
    /// still reads the final revision afterwards. Cancel-safe.
    pub async fn state_changed(&mut self) -> Result<Arc<PublishedBook>, WriterGone> {
        if self.state.changed().await.is_err() {
            self.state_open = false;
            return Err(WriterGone);
        }
        Ok(Arc::clone(&self.state.borrow_and_update()))
    }

    /// Restarts this attachment from the authoritative latest state.
    ///
    /// This is the documented answer to [`ObserverRecvError::ContinuityLost`] in either of
    /// its forms, and the only thing that clears it — and the only thing that moves this
    /// attachment onto a new continuity epoch. The returned book is at some revision R, every buffered
    /// mutation is discarded, and every later mutation at or below R is suppressed because
    /// that state already contains it. Mutations above R resume normally, so the consumer
    /// neither misses nor double-applies anything across the boundary. The boundary the
    /// returned state stops at is recorded afresh, so overruns are judged against this
    /// attachment rather than the one it replaced.
    pub fn reattach(&mut self) -> Arc<PublishedBook> {
        self.deliveries = self.deliveries.resubscribe();
        let published = Arc::clone(&self.state.borrow_and_update());
        self.resume_after = published.revision();
        self.boundary = attachment_cursor(&published);
        self.cursor = self.boundary.clone();
        self.continuous = true;
        self.pending = None;
        self.lost = None;
        published
    }

    /// Takes the next delivery past this attachment's boundary without waiting.
    ///
    /// Returns `Ok(None)` when the ring holds nothing new. Fails with
    /// [`ObserverRecvError::ContinuityLost`] once the writer has overtaken this consumer —
    /// and with the same error on every later call until [`Self::reattach`], so partial
    /// history is never applied — and with [`ObserverRecvError::Closed`] once the writer is
    /// gone and the ring is drained. An overrun is judged, not assumed: it is carried
    /// unresolved until the delivery that resumes the ring names the positions it dropped,
    /// and one that dropped nothing this attachment's state lacks is passed over silently.
    pub fn try_recv(&mut self) -> Result<Option<StreamDelivery>, ObserverRecvError> {
        if let Some(error) = &self.lost {
            return Err(error.clone());
        }
        loop {
            match self.deliveries.try_recv() {
                Ok(delivery) => {
                    if let Some(delivery) = self.admit(delivery)? {
                        return Ok(Some(delivery));
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => return Ok(None),
                Err(broadcast::error::TryRecvError::Closed) => return Err(self.closed()),
                Err(broadcast::error::TryRecvError::Lagged(missed)) => self.overran(missed),
            }
        }
    }

    /// Waits for the next delivery past this attachment's boundary.
    ///
    /// Fails exactly as [`Self::try_recv`] does, minus the empty case: a lost continuity is
    /// returned immediately and keeps being returned until [`Self::reattach`]. Cancel-safe.
    pub async fn recv(&mut self) -> Result<StreamDelivery, ObserverRecvError> {
        if let Some(error) = &self.lost {
            return Err(error.clone());
        }
        loop {
            match self.deliveries.recv().await {
                Ok(delivery) => {
                    if let Some(delivery) = self.admit(delivery)? {
                        return Ok(delivery);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return Err(self.closed()),
                Err(broadcast::error::RecvError::Lagged(missed)) => self.overran(missed),
            }
        }
    }

    /// Waits on both surfaces at once, for a consumer that tracks state and history
    /// together.
    ///
    /// Deliveries are taken first whenever both are ready, so pending history drains before
    /// the coalescing state notification fires. Once the state surface closes this degrades
    /// to [`Self::recv`], so buffered deliveries still drain before
    /// [`ObserverRecvError::Closed`]. Fails exactly as [`Self::recv`] does, refusing every
    /// event after a continuity loss until [`Self::reattach`]. Cancel-safe.
    pub async fn next_event(&mut self) -> Result<ObserverEvent, ObserverRecvError> {
        if let Some(error) = &self.lost {
            return Err(error.clone());
        }
        loop {
            if !self.state_open {
                return self.recv().await.map(ObserverEvent::from);
            }
            let waited = tokio::select! {
                biased;
                delivery = self.deliveries.recv() => Waited::Delivery(delivery),
                changed = self.state.changed() => Waited::State(changed),
            };
            match waited {
                Waited::Delivery(Ok(delivery)) => {
                    if let Some(delivery) = self.admit(delivery)? {
                        return Ok(ObserverEvent::from(delivery));
                    }
                }
                Waited::Delivery(Err(broadcast::error::RecvError::Closed)) => {
                    return Err(self.closed());
                }
                Waited::Delivery(Err(broadcast::error::RecvError::Lagged(missed))) => {
                    self.overran(missed);
                }
                Waited::State(Ok(())) => {
                    return Ok(ObserverEvent::Published(Arc::clone(
                        &self.state.borrow_and_update(),
                    )));
                }
                Waited::State(Err(_)) => self.state_open = false,
            }
        }
    }

    /// This consumer's own state.
    ///
    /// [`ConsumerState::Overrun`] persists until [`Self::reattach`], because an overtaken
    /// consumer's next raw mutation is not continuous with its last one. An overrun still
    /// awaiting classification is not one: until it is proven to have dropped something
    /// this attachment needed, the consumer stays [`ConsumerState::Attached`] and
    /// continuous. [`ConsumerState::Rebased`] is the other stopped state and is equally
    /// cleared only by [`Self::reattach`]: no delivery was dropped, but the writer rebased
    /// the stream onto a new continuity epoch, so this attachment's cursor no longer
    /// describes a position in it. Both report the cursor the attachment stopped at.
    /// [`ConsumerState::Detached`] is terminal: a gone writer never returns. `continuous`
    /// reports whether every mutation since this attachment began was delivered.
    pub fn state(&self) -> ConsumerState {
        if self.closed {
            ConsumerState::Detached
        } else if matches!(
            self.lost,
            Some(ObserverRecvError::ContinuityLost {
                reason: ContinuityReason::RecoveryBase,
                ..
            })
        ) {
            ConsumerState::Rebased {
                cursor: self.cursor.clone(),
            }
        } else if self.lost.is_some() {
            ConsumerState::Overrun {
                cursor: self.cursor.clone(),
            }
        } else {
            ConsumerState::Attached {
                cursor: self.cursor.clone(),
                continuous: self.continuous,
            }
        }
    }

    /// The single ordered decision every received delivery passes through, on all three
    /// receive paths: redundant, fenced by an epoch change, or the consumer's next delivery.
    ///
    /// The order is what makes each answer truthful, because an unresolved overrun and an
    /// epoch change can arrive on the same delivery.
    ///
    /// 1. A redundant delivery is already contained in the state this attachment read, per
    ///    [`Self::redundant`]. The ring is FIFO, so everything an unresolved overrun span
    ///    dropped preceded this delivery and is therefore contained in that state too: the
    ///    span is cleared as harmless and the delivery passed over silently.
    /// 2. Otherwise a delivery from another epoch means the writer replaced the book
    ///    wholesale and published that as state rather than as mutations, so this attachment
    ///    cannot go on applying the stream. The rebase dominates any pending overrun,
    ///    because restarting is the answer to both: the loss reports
    ///    [`ContinuityReason::RecoveryBase`] carrying whatever the span had dropped, so a
    ///    nonzero `missed` there says deliveries were dropped *as well as* the rebase, and
    ///    [`Self::reattach`] recovers from both at once.
    /// 3. Otherwise the delivery continues this attachment's own epoch, and any pending
    ///    overrun is judged against the boundary exactly as [`classify_overrun`] describes.
    ///
    /// A loss raised here is sticky: every later receive repeats it until
    /// [`Self::reattach`], the only thing that moves an attachment onto a new epoch.
    fn admit(
        &mut self,
        delivery: StreamDelivery,
    ) -> Result<Option<StreamDelivery>, ObserverRecvError> {
        if self.redundant(&delivery) {
            self.pending = None;
            return Ok(None);
        }
        if delivery.cursor().epoch() != self.boundary.epoch() {
            let missed = self.pending.take().map_or(0, PendingOverrun::missed);
            return Err(self.fail(rebased(missed)));
        }
        if let Some(error) = self.resolve(Some(delivery.cursor())) {
            return Err(error);
        }
        self.cursor = delivery.cursor().clone();
        Ok(Some(delivery))
    }

    /// Whether this attachment's own state already contains `delivery`.
    ///
    /// The two kinds are judged by different evidence because they change different things.
    /// A mutation is contained in every state at or above the revision its commit produced,
    /// so the revision decides. A resolution advances no revision — it carries the revision
    /// it was ordered after — so a revision test would call every resolution arriving at the
    /// attachment's own revision redundant and drop a live one silently. What contains a
    /// resolution is the boundary position instead: everything strictly below the boundary
    /// on `(epoch, position)` was already delivered before this attachment began, and
    /// everything at or above it is this attachment's to receive. The comparison is
    /// lexicographic because positions restart at 0 on every new continuity epoch.
    fn redundant(&self, delivery: &StreamDelivery) -> bool {
        match delivery {
            StreamDelivery::Mutation(delivery) => delivery.revision() <= self.resume_after,
            StreamDelivery::Resolution(delivery) => {
                let cursor = delivery.cursor();
                (cursor.epoch(), cursor.position())
                    < (self.boundary.epoch(), self.boundary.position())
            }
        }
    }

    fn fail(&mut self, error: ObserverRecvError) -> ObserverRecvError {
        self.lost = Some(error.clone());
        self.continuous = false;
        error
    }

    fn closed(&mut self) -> ObserverRecvError {
        if let Some(error) = self.resolve(None) {
            return error;
        }
        self.closed = true;
        ObserverRecvError::Closed
    }

    fn overran(&mut self, missed: u64) {
        self.pending = Some(self.pending.unwrap_or_default().accumulated(missed));
    }

    /// Judges any unresolved overrun against `resumed`, freezing the loss when it is not
    /// provably harmless.
    ///
    /// The pending span is cleared either way: a harmless overrun leaves the attachment
    /// continuous, and a lost one becomes the sticky failure every later receive repeats
    /// until [`Self::reattach`].
    fn resolve(&mut self, resumed: Option<&MutationCursor>) -> Option<ObserverRecvError> {
        let pending = self.pending.take()?;
        match classify_overrun(&self.boundary, resumed) {
            OverrunVerdict::Harmless => None,
            OverrunVerdict::Lost => Some(self.fail(continuity_lost(pending.missed()))),
        }
    }
}

fn continuity_lost(missed: u64) -> ObserverRecvError {
    ObserverRecvError::ContinuityLost {
        reason: ContinuityReason::Overrun,
        missed,
    }
}

/// The stream this attachment was following was rebased onto a new continuity epoch.
///
/// `missed` is whatever an unresolved overrun span had dropped when the rebase was found:
/// zero when the rebase is the whole story, and nonzero when deliveries were dropped as
/// well. Either way the rebase is the actionable fact and restarting answers both.
fn rebased(missed: u64) -> ObserverRecvError {
    ObserverRecvError::ContinuityLost {
        reason: ContinuityReason::RecoveryBase,
        missed,
    }
}

/// The stream position an attachment starts from: the next position the writer will emit,
/// or the start of the epoch when the published stream is lost.
fn attachment_cursor(published: &PublishedBook) -> MutationCursor {
    match published.continuity() {
        MutationContinuity::Intact {
            epoch,
            next_position,
        } => MutationCursor::new(*epoch, *next_position),
        MutationContinuity::Lost { epoch, .. } => MutationCursor::new(*epoch, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BoundedSourceEvidence, ConnectionIdentity, DeliveryPath, LocalMonotonicTimestamp,
        MarketRef, NativeIdentifierKind, NativeLabel, NativeMarketKey, NativeOutcome, Origin,
        ProvenanceInput, ReplicaRole, Representation, ResolutionObservation,
        SourceEvidenceCapacity, SourceTimestamp, Venue,
    };

    fn market() -> MarketRef {
        MarketRef::new(
            Venue::new("limitless").expect("venue"),
            NativeMarketKey::new(NativeIdentifierKind::slug(), "resolving").expect("key"),
        )
    }

    fn resolution() -> Arc<MarketResolution> {
        let provenance = crate::Provenance::new(ProvenanceInput {
            market: market(),
            outcome: None,
            native_family: "marketResolved".into(),
            source_timestamp: Some(SourceTimestamp::new("2026-09-01T12:05:00Z").expect("stamp")),
            source_evidence: BoundedSourceEvidence::new(
                Vec::new(),
                SourceEvidenceCapacity::new(0).expect("capacity"),
            )
            .expect("evidence"),
            daemon_generation: 1,
            connection: ConnectionIdentity::new("ws.limitless.exchange", 1).expect("connection"),
            subscription_generation: 1,
            receive_position: 0,
            commit_position: 0,
            local_receive_time: LocalMonotonicTimestamp::new(0),
            local_commit_time: LocalMonotonicTimestamp::new(0),
            replica: ReplicaRole::PublishingPrimary,
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: 0,
            continuity_epoch: 0,
        })
        .expect("provenance");
        let observation = ResolutionObservation::new(
            provenance,
            NativeOutcome::venue_defined("Yes").expect("outcome"),
            NativeLabel::new("clob").expect("label"),
            DeliveryPath::MarketFeed,
        )
        .expect("observation");
        Arc::new(MarketResolution::new(
            observation,
            1,
            SourceTimestamp::new("2026-09-01T12:05:00Z").expect("stamp"),
        ))
    }

    /// An attachment whose boundary sits at `(epoch, position)` and whose state contains
    /// every revision up to `resume_after`.
    ///
    /// Set directly, because the one window that produces a delivery below an attachment's
    /// own boundary is the instant between a writer's send and its state publication, which
    /// no caller of the public surface can be scheduled into: `attach` borrows the writer
    /// shared and publishing borrows it exclusively. The rule still has to be right, so it
    /// is pinned here rather than left to a window a test cannot enter.
    fn attached_at(
        writer: &BookWriter,
        epoch: u64,
        position: u64,
        resume_after: u64,
    ) -> BookObserver {
        let mut observer = writer.attach();
        observer.boundary = MutationCursor::new(epoch, position);
        observer.cursor = observer.boundary.clone();
        observer.resume_after = resume_after;
        observer
    }

    fn delivery(epoch: u64, position: u64, revision: u64) -> StreamDelivery {
        StreamDelivery::Resolution(ResolutionDelivery {
            revision,
            cursor: MutationCursor::new(epoch, position),
            resolution: resolution(),
        })
    }

    /// A resolution below the attachment's boundary is already contained in the state it
    /// read: it is passed over silently, and it never drags the cursor backwards.
    ///
    /// Judged by revision it would instead be admitted — a resolution carries a revision the
    /// attachment may well not have read yet — and the attachment would go on from a
    /// position it had already passed.
    #[test]
    fn observer_passes_over_a_resolution_below_its_boundary() {
        let writer = BookWriter::new(
            OrderBook::new(market()),
            ObserverCapacity::new(8).expect("capacity"),
        );
        let mut observer = attached_at(&writer, 3, 7, 4);
        assert_eq!(observer.admit(delivery(3, 6, 9)), Ok(None));
        assert_eq!(
            observer.state(),
            ConsumerState::Attached {
                cursor: MutationCursor::new(3, 7),
                continuous: true
            }
        );
    }

    /// A resolution from an older epoch is equally contained, and is never mistaken for the
    /// rebase a *newer* epoch means.
    #[test]
    fn observer_passes_over_a_resolution_from_an_older_epoch() {
        let writer = BookWriter::new(
            OrderBook::new(market()),
            ObserverCapacity::new(8).expect("capacity"),
        );
        let mut observer = attached_at(&writer, 3, 7, 4);
        assert_eq!(observer.admit(delivery(2, 9_000, 9)), Ok(None));
        assert!(matches!(
            observer.state(),
            ConsumerState::Attached {
                continuous: true,
                ..
            }
        ));
    }

    /// An overrun span resolved by a passed-over resolution dropped nothing this attachment
    /// needed, so it is cleared as harmless rather than raised as a loss.
    #[test]
    fn observer_clears_a_pending_overrun_on_a_passed_over_resolution() {
        let writer = BookWriter::new(
            OrderBook::new(market()),
            ObserverCapacity::new(8).expect("capacity"),
        );
        let mut observer = attached_at(&writer, 3, 7, 4);
        observer.overran(5);
        assert_eq!(observer.admit(delivery(3, 2, 9)), Ok(None));
        assert!(observer.pending.is_none());
        assert!(matches!(
            observer.state(),
            ConsumerState::Attached {
                continuous: true,
                ..
            }
        ));
    }

    /// A resolution at the boundary itself is this attachment's to receive, whatever
    /// revision it carries.
    #[test]
    fn observer_admits_a_resolution_at_its_boundary() {
        let writer = BookWriter::new(
            OrderBook::new(market()),
            ObserverCapacity::new(8).expect("capacity"),
        );
        let mut observer = attached_at(&writer, 3, 7, 9);
        let admitted = observer.admit(delivery(3, 7, 4)).expect("no loss");
        assert_eq!(
            admitted.map(|delivery| delivery.cursor().clone()),
            Some(MutationCursor::new(3, 7))
        );
    }

    /// `publish_resolution` leaves the published boundary past the position it sent, so a
    /// later attachment judges that resolution redundant instead of skipping it silently.
    ///
    /// Both halves of the ordering are pinned here because the interleaving itself cannot be
    /// entered from the public surface: `attach` borrows the writer shared and publishing
    /// borrows it exclusively, so no test can be scheduled between the send and the
    /// publication. What is observable is the state the pair leaves behind — an attachment
    /// taken before the call receives the resolution at P, and one taken after starts at P+1
    /// and rules a delivery at P already contained in the state it read. Sending first would
    /// leave the later attachment at P, unable to receive the resolution the ring had already
    /// carried past it, and it would admit the next delivery as continuous.
    #[test]
    fn a_published_resolution_leaves_the_boundary_past_the_position_it_sent() {
        let mut writer = BookWriter::new(
            OrderBook::new(market()),
            ObserverCapacity::new(8).expect("capacity"),
        );
        let mut before = writer.attach();
        let delivery = writer
            .publish_resolution(resolution())
            .expect("an intact stream allocates a position");
        assert_eq!(delivery.cursor(), &MutationCursor::new(0, 0));
        assert_eq!(
            before.try_recv(),
            Ok(Some(StreamDelivery::Resolution(delivery.clone())))
        );

        let mut after = writer.attach();
        assert_eq!(
            after.state(),
            ConsumerState::Attached {
                cursor: MutationCursor::new(0, 1),
                continuous: true
            }
        );
        assert_eq!(
            after.admit(StreamDelivery::Resolution(delivery)),
            Ok(None),
            "the state the later attachment read already contains the resolution"
        );
    }

    /// A resolution from a newer epoch is the rebase, exactly as a mutation from one is.
    #[test]
    fn observer_reports_a_rebase_on_a_newer_epoch_resolution() {
        let writer = BookWriter::new(
            OrderBook::new(market()),
            ObserverCapacity::new(8).expect("capacity"),
        );
        let mut observer = attached_at(&writer, 3, 7, 4);
        assert_eq!(
            observer.admit(delivery(4, 0, 9)),
            Err(ObserverRecvError::ContinuityLost {
                reason: ContinuityReason::RecoveryBase,
                missed: 0
            })
        );
    }
}
