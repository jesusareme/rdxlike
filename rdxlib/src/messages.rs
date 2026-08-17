//! The envelope every unit of work travels in on its way to the runtime.

/// A unit of work queued for the runtime.
///
/// Both variants share one channel so ordering between model changes and runtime changes
/// is preserved.
///
/// TODO: note that clients normally build these through the `Into` impls on their own
/// action types rather than naming this enum.
#[derive(Debug, PartialEq)]
pub enum Message<A, R>
{
	Action(A),
	Runtime(R),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Operation<M> {
	Run(M),
	Stop,
}