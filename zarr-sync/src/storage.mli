module IO : Zarr.Types.IO with type 'a t = 'a

(** A blocking I/O in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with type 'a io := 'a

(** A blocking I/O Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with type 'a io := 'a

(** A blocking I/O local filesystem storage backend for a Zarr v3 hierarchy. *)
module FilesystemStore : sig
  type error = [ `Read of string | `Write of string ]
  include Zarr.Storage.S with type error := error and type 'a io := 'a

  val create : ?perm:int -> string -> (t, [> `Zarr of [> `Write of string ]]) result
  (** [create ~perm dir] creates a new filesystem store.*)

  val open_store : ?perm:int -> string -> (t, [> `Zarr of [> `Read of string ]]) result
  (** [open_store ~perm dir] create a handle an existing filesystem Zarr store stored at path [dir]. *)
end
