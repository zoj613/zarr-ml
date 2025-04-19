module IO : Zarr.Types.IO with type 'a t = 'a

(** An Eio-aware in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with type 'a io := 'a

(** An Eio-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with type 'a io := 'a

(** An Eio-aware local filesystem storage backend for a Zarr v3 hierarchy. *)
module FilesystemStore : sig
  type error = [ `Read of string | `Write of string ]
  include Zarr.Storage.S with type error := error and type 'a io := 'a

  val create : ?perm:int -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> (t, [> `Zarr of [> `Write of string ]]) result
  (** [create ~perm ~env dir] returns a new filesystem store. *)

  val open_store : ?perm:int -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> (t, [> `Zarr of [> `Read of string ]]) result
  (** [open_store ~perm ~env dir] returns an existing filesystem Zarr store. *)
end
