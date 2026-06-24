use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum RingBufferError {
    #[error("Buffer is full — producer is too fast")]
    BufferFull,

    #[error("Buffer is empty — consumer is too fast")]
    BufferEmpty,

    #[error("Capacity must be a power of 2, got {0}")]
    InvalidCapacity(usize),

    #[error("Pipeline stage '{stage}' failed: {reason}")]
    PipelineError { stage: String, reason: String },
}
