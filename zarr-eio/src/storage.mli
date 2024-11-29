module IO : Zarr.Types.IO with type 'a t = 'a

(** An Eio-aware in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with type 'a io := 'a

(** An Eio-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with type 'a io := 'a

(** An Eio-aware local filesystem storage backend for a Zarr v3 hierarchy. *)
module FilesystemStore : sig
  include Zarr.Storage.S with type 'a io := 'a

  val create : ?perm:Eio.File.Unix_perm.t -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> t
  (** [create ~perm ~env dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:Eio.File.Unix_perm.t -> env:<fs : Eio.Fs.dir_ty Eio.Path.t; ..> -> string -> t
  (** [open_store ~perm ~env dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is a file and not a Zarr store path. *)
end

module HttpStore : sig
  exception Not_implemented
  exception Request_failed of int * string
  include Zarr.Storage.STORE with module Deferred = Deferred
  val with_open :
    net:_ Eio.Net.t ->
    Uri.t ->
    (t -> 'a) ->
    'a
end
