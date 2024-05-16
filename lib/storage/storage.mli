type error = Interface.error

module type S = Interface.S

module MemoryStore : sig 
  include S
  val create : unit -> t
end

module FilesystemStore : sig
  include S
  val create
    : ?file_perm:Unix.file_perm -> string -> t
  val open_store
    : ?file_perm:Unix.file_perm -> string -> (t, [> error]) result
  val open_or_create
    : ?file_perm:Unix.file_perm -> string -> (t, [> error]) result
end
