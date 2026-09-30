About Send:

- Rc is not Send, because it is a reference to an internal state storing somewhere else. when if we pass a cloned Rc to another thread, both threads can still have reference to an internal state, i.e the ref_count. if both clone further at the same time, we might risk racing the ref_count

- similarly, NonNull and raw pointers are not Send. but the skiplist is Send because we don't expose raw pointer operation outside.

- we need SkipList to be Send because we wrap Redis inside Arc<Mutex>. Mutex needs the wrapped type to be Send so it can be access in different threads. if we wrap Rc inside Mutex and send them around, it is still racy.
