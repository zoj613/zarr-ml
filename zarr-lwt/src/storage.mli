module Deferred : Zarr.Types.Deferred with type 'a t = 'a Lwt.t

(** An Lwt-aware in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with module Deferred = Deferred

(** An Lwt-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with module Deferred = Deferred

(** An Lwt-aware local filesystem storage backend for a Zarr V3 hierarchy. *)
module FilesystemStore : sig
  include Zarr.Storage.STORE with module Deferred = Deferred

  val create : ?perm:Unix.file_perm -> string -> t
  (** [create ~perm dir] returns a new filesystem store.

      @raise Failure if [dir] is a directory that already exists.*)

  val open_store : ?perm:Unix.file_perm -> string -> t
  (** [open_store ~perm dir] returns an existing filesystem Zarr store.

      @raise Failure if [dir] is not a Zarr store path. *)
end

(** An Lwt-aware Amazon S3 bucket storage backend for a Zarr V3 hierarchy. *)
module AmazonS3Store : sig
  exception Request_failed of Aws_s3_lwt.S3.error

  include Zarr.Storage.STORE with module Deferred = Deferred

  val with_open :
    ?scheme:[ `Http | `Https ] ->
    ?inet:[ `V4 | `V6 ] ->
    ?retries:int ->
    region:Aws_s3.Region.t ->
    bucket:string ->
    profile:string ->
    (t -> 'a Lwt.t) ->
    'a Lwt.t
  (** [with_open ~region ~bucket ~profile f] opens an S3 bucket store with
      bucket name [bucket] at region [region] using credentials specified by
      profile [profile]. The credentials are read locally from a [~/.aws/credentials]
      file or from an IAM service if the profile or file is not available.
      Function [f] is applied to the store's open handle and its output is
      returned to the caller.

      {ul 
      {- [scheme] is the HTTP scheme to use when connecting to S3, and must be
        one of [`Http | `Https]. Defaults to [`Http].}
      {- [inet] is the IP version and must be one of [`V4 | `V6]. Defaults to [`V4].}
      {- [retries] is the number of times to retry a request should it return an error.}
      }

      @raise Request_failed if an error occurs while sending a request to the S3 service. *)
end
