# Welcome to Delta Kernel

Delta kernel is a query-engine agnostic library to provide Delta support in any engine.

There are currently two implementations of the Delta kernel: a native (rust) implementation (with a
C/C++ FFI): [delta-kernel-rs] (this project!) and a Java implementation [delta-kernel-java]. This
book is only concerned with the rust implementation.

```
     ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─         ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
       ┌──────────────┐     │         ┌───────────────────────┐ │
     │ │ something    │              ││                       │
       └──────────────┘     │         └───────────────────────┘ │
     │ ┌────────────────┐            │┌───────────────────────┐
       │  something     │   │ ◀────▶  │                       │ │
     │ └────────────────┘            │└───────────────────────┘
                ...         │                   ...             │
     │ ┌──────────────────┐          │ ┌──────────────────┐
       │                  │ │          │                  │     │
     │ └──────────────────┘          │ └──────────────────┘
      ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```


[delta-kernel-rs]: https://github.com/delta-io/delta-kernel-rs
[delta-kernel-java]: https://github.com/delta-io/delta/tree/master/kernel