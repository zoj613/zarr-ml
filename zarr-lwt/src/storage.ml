module MemoryStore = struct
  include Zarr.Storage.Make(Zarr.Memory.Make(Deferred))
  let create = Zarr.Memory.create
end

module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred
    open Deferred.Infix
    open Deferred.Syntax

    type t = {dirname : string; perm : Lwt_unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos @@ String.length path - pos

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let parent_dir = Filename.dirname fn in
      Lwt_unix.file_exists parent_dir >>= function
      | false ->
        let* () = create_parent_dir parent_dir perm in
        Lwt_unix.mkdir parent_dir perm
      | true -> Lwt.return_unit

    let size t key =
      Lwt_io.file_length (key_to_fspath t key) >>| Int64.to_int

    let get t key =
      let* bsize = size t key in
      Lwt.catch
        (fun () ->
          Lwt_io.with_file
            ~buffer:(Lwt_bytes.create bsize)
            ~flags:Unix.[O_RDONLY; O_NONBLOCK]
            ~perm:t.perm
            ~mode:Lwt_io.Input
            (key_to_fspath t key)
            Lwt_io.read)
        (function
        | Unix.Unix_error (Unix.ENOENT, _, _) ->
          raise @@ Zarr.Storage.Key_not_found key
        | exn -> raise exn)

    let get_partial_values t key ranges =
      let* tot = size t key in
      let l = List.fold_left
        (fun a (s, l) ->
          Option.fold ~none:(Int.max a (tot - s)) ~some:(Int.max a) l) 0 ranges
      in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create l)
        ~flags:Unix.[O_RDONLY; O_NONBLOCK]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        @@ fun ic ->
          Lwt_list.map_s
            (fun (ofs, len) -> 
              let count = Option.fold ~none:(tot - ofs) ~some:Fun.id len in
              let* () = Lwt_io.set_position ic @@ Int64.of_int ofs in
              Lwt_io.read ~count ic) ranges

    let set t key value =
      let filename = key_to_fspath t key in
      let* () = create_parent_dir filename t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create @@ String.length value)
        ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT; O_NONBLOCK]
        ~perm:t.perm
        ~mode:Lwt_io.Output
        filename
        (Fun.flip Lwt_io.write value)

    let set_partial_values t key ?(append=false) rvs =
      let l = List.fold_left (fun a (_, s) -> Int.max a (String.length s)) 0 rvs in
      let flags = Unix.[O_NONBLOCK; O_WRONLY] in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create l)
        ~flags:(if append then Unix.O_APPEND :: flags else flags)
        ~perm:t.perm
        ~mode:Lwt_io.Output
        (key_to_fspath t key)
        @@ fun oc ->
          Lwt_list.iter_s
            (fun (ofs, value) ->
              let* () = Lwt_io.set_position oc @@ Int64.of_int ofs in
              Lwt_io.write oc value) rvs

    let rec walk t acc dir =
      Lwt_stream.fold_s
        (fun x a -> 
          if x = "." || x  = ".." then Lwt.return a else
          match Filename.concat dir x with
          | p when Sys.is_directory p -> walk t a p
          | p -> Lwt.return @@ fspath_to_key t p :: a)
        (Lwt_unix.files_of_directory dir) acc

    let list t = walk t [] (key_to_fspath t "")

    let list_prefix t prefix =
      walk t [] (key_to_fspath t prefix)

    let is_member t key =
      Lwt_unix.file_exists @@ key_to_fspath t key

    let erase t key =
      Lwt_unix.unlink @@ key_to_fspath t key

    let erase_prefix t pre =
      list_prefix t pre >>= Lwt_list.iter_s @@ erase t

    let list_dir t prefix =
      let dir = key_to_fspath t prefix in
      let+ files =
        Lwt_stream.to_list @@ Lwt_stream.filter
          (fun x -> if x = "." || x = ".." then false else true)
          (Lwt_unix.files_of_directory dir) in
      List.partition_map
        (fun x ->
          match Filename.concat dir x with
          | p when Sys.is_directory p ->
            Either.right @@ (fspath_to_key t p) ^ "/"
          | p -> Either.left @@ fspath_to_key t p) files

    let rename t k k' =
      Lwt_unix.rename (key_to_fspath t k) (key_to_fspath t k')
  end

  module U = Zarr.Util

  let create ?(perm=0o700) dirname =
    U.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    FS.{dirname = U.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname =
    if Sys.is_directory dirname
    then FS.{dirname = U.sanitize_dir dirname; perm}
    else raise @@ Zarr.Storage.Not_a_filesystem_store dirname

  include Zarr.Storage.Make(FS)
end
