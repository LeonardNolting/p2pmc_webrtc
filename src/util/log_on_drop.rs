#[derive(Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub(crate) struct LogOnDrop<T> {
    pub(crate) inner: T,
    pub(crate) description: &'static str,
}

impl <T> LogOnDrop<T> {
    pub(crate) fn new(inner: T, description: &'static str) -> LogOnDrop<T> {
        LogOnDrop { inner, description }
    }
}

impl<T> Drop for LogOnDrop<T> {
    fn drop(&mut self) {
        println!("Dropping {}", self.description);
    }
}

fn main() {
    let _a = LogOnDrop::new((), "a");
    let _b = LogOnDrop::new((), "b");
    let _c = LogOnDrop::new((), "c");
}