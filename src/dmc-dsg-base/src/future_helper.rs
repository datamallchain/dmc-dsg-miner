use async_std::task::Waker;
use std::sync::{Mutex, Arc};
use async_std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Context};

pub struct SimpleFutureState<RESULT> {
    waker: Option<Waker>,
    result: Option<RESULT>,
}

impl <RESULT> SimpleFutureState<RESULT> {
    pub fn new() -> Arc<Mutex<SimpleFutureState<RESULT>>> {
        Arc::new(Mutex::new(SimpleFutureState {
            waker: None,
            result: None
        }))
    }

    pub fn set_complete(state: &Arc<Mutex<SimpleFutureState<RESULT>>>, result: RESULT) {
        let mut state = state.lock().unwrap();
        state.result = Some(result);
        if state.waker.is_some() {
            state.waker.take().unwrap().wake();
        }
    }
}

pub struct SimpleFuture<RESULT>(Arc<Mutex<SimpleFutureState<RESULT>>>);

impl <RESULT> SimpleFuture<RESULT> {
    pub fn new(state: Arc<Mutex<SimpleFutureState<RESULT>>>) -> Self {
        Self(state)
    }
}

impl <RESULT> Future for SimpleFuture<RESULT> {
    type Output = RESULT;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.0.lock().unwrap();
        if state.result.is_some() {
            return Poll::Ready(state.result.take().unwrap());
        }

        if state.waker.is_none() {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}
