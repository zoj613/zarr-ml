module IO : Zarr.Types.IO with type 'a t = 'a Lwt.t

(** An Lwt-aware in-memory storage backend for Zarr v3 hierarchy. *)
module MemoryStore : Zarr.Memory.S with type 'a io := 'a Lwt.t

(** An Lwt-aware Zip file storage backend for a Zarr v3 hierarchy. *)
module ZipStore : Zarr.Zip.S with type 'a io := 'a Lwt.t

(** An Lwt-aware local filesystem storage backend for a Zarr V3 hierarchy. *)
module FilesystemStore : sig
  include Zarr.Storage.S
    with type error = [ `Read of string | `Write of string ]
    and type 'a io := 'a Lwt.t

  val create : ?perm:int -> string -> (t, [> `Zarr of [> `Write of string ]]) result Lwt.t
  (** [create ~perm dir] returns a new filesystem store. *)

  val open_store : ?perm:int -> string -> (t, [> `Zarr of [> `Read of string ]]) result
  (** [open_store ~perm dir] returns an existing filesystem Zarr store. *)
end

(** An Lwt-aware Amazon S3 bucket storage backend for a Zarr V3 hierarchy. *)
module AmazonS3Store : sig
  include Zarr.Storage.S
    with type error = [ `Request_failed of Aws_s3_lwt.S3.error ]
    and type 'a io := 'a Lwt.t

  val with_open :
    ?scheme:[ `Http | `Https ] ->
    ?inet:[ `V4 | `V6 ] ->
    ?retries:int ->
    region:Aws_s3.Region.t ->
    bucket:string ->
    profile:string ->
      (t -> ('a, [> `Zarr of [> error ] ] as 'b) result Lwt.t) ->
    ('a, 'b) result Lwt.t
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
      } *)
end
