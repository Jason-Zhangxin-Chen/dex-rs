use crate::message::side_path::ReplicationMsg;

/// Book state listener push the changes of the book to the remote OMS_Slave node via NATS.
pub type BookListener = Box<dyn Fn(&ReplicationMsg)>;

/// Listeners collect a set of callback closure to notify engine event to external system.
/// They are none blocking functions.
#[derive(Default)]
pub struct Listeners {
    /// Book state listener, it replicates the changes of the book to
    /// the remote [SVD_OMS_Slave].
    book_listener: Option<BookListener>,
}

impl Listeners {
    /// Set the book state listener.
    pub fn with_book_state_listener(mut self, book_listener: BookListener) -> Self {
        self.book_listener = Some(book_listener);
        self
    }
}
