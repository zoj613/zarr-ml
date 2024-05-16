type error = Interface.error

module type S = Interface.S

module MemoryStore = struct
  module MS = Interface.Make (Memory.Impl)
  let create = Memory.create
  include MS
end

module FilesystemStore = struct
  module FS = Interface.Make (Filesystem.Impl)
  let create = Filesystem.create
  let open_store = Filesystem.open_store
  let open_or_create = Filesystem.open_or_create
  include FS
end
