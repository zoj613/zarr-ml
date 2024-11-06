module MemoryStore = struct
  module M = Zarr.Memory.Make(Deferred)
  include Zarr.Storage.Make(M)
  let create = M.create
end

module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred
    open Deferred.Infix
    open Deferred.Syntax

    type t = {dirname : string; perm : Lwt_unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos (String.length path - pos)

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let parent_dir = Filename.dirname fn in
      Lwt_unix.file_exists parent_dir >>= function
      | true -> Lwt.return_unit
      | false ->
        let* () = create_parent_dir parent_dir perm in
        Lwt_unix.mkdir parent_dir perm

    let size t key =
      Lwt.catch
        (fun () ->
          let+ length = Lwt_io.file_length (key_to_fspath t key) in
          Int64.to_int length)
        (fun _ -> Deferred.return 0)

    let get t key =
      let* buf_size = size t key in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create buf_size)
        ~flags:[Unix.O_RDONLY]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        Lwt_io.read

    let get_partial_values t key ranges =
      let max_range ~tot acc (s, l) = match l with
        | None -> Int.max acc (tot - s) 
        | Some rs -> Int.max acc rs
      in
      let read_range ~tot ~ic (ofs, len) =
        let* () = Lwt_io.set_position ic (Int64.of_int ofs) in
        match len with
        | None -> Lwt_io.read ~count:(tot - ofs) ic
        | Some count -> Lwt_io.read ~count ic
      in
      let* tot = size t key in
      let buf_size = List.fold_left (max_range ~tot) 0 ranges in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create buf_size)
        ~flags:[Unix.O_RDONLY]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        (fun ic -> Lwt_list.map_s (read_range ~tot ~ic) ranges)

    let set t key value =
      let write ~value oc = Lwt_io.write oc value in
      let filename = key_to_fspath t key in
      let* () = create_parent_dir filename t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create (String.length value))
        ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT]
        ~perm:t.perm
        ~mode:Lwt_io.Output
        filename
        (write ~value)

    let set_partial_values t key ?(append=false) rvs =
      let write ~oc (ofs, value) =
        let* () = Lwt_io.set_position oc (Int64.of_int ofs) in
        Lwt_io.write oc value
      in
      let l = List.fold_left (fun a (_, s) -> Int.max a (String.length s)) 0 rvs in
      let flags = match append with
        | false -> Unix.[O_WRONLY; O_CREAT] 
        | true -> Unix.[O_APPEND; O_WRONLY; O_CREAT] 
      in
      let filepath = key_to_fspath t key in
      let* () = create_parent_dir filepath t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create l)
        ~perm:t.perm
        ~mode:Lwt_io.Output
        ~flags
        filepath
        (fun oc -> Lwt_list.iter_s (write ~oc) rvs)

    let rec walk t acc dir =
      let accumulate ~t x a =
        if x = "." || x  = ".." then Lwt.return a else
        match Filename.concat dir x with
        | p when Sys.is_directory p -> walk t a p
        | p -> Lwt.return (fspath_to_key t p :: a)
      in
      Lwt_stream.fold_s (accumulate ~t) (Lwt_unix.files_of_directory dir) acc

    let list t = walk t [] (key_to_fspath t "")

    let list_prefix t prefix = walk t [] (key_to_fspath t prefix)

    let is_member t key = Lwt_unix.file_exists (key_to_fspath t key)

    let erase t key = Lwt_unix.unlink (key_to_fspath t key)

    let erase_prefix t pre = list_prefix t pre >>= Lwt_list.iter_s (erase t)

    let list_dir t prefix =
      let choose ~t ~dir x = match Filename.concat dir x with
        | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let predicate x = if x = "." || x = ".." then false else true in
      let dir = key_to_fspath t prefix in
      let relevant = Lwt_stream.filter predicate (Lwt_unix.files_of_directory dir) in
      let+ dir_contents = Lwt_stream.to_list relevant in
      List.partition_map (choose ~t ~dir) dir_contents

    let rename t k k' = Lwt_unix.rename (key_to_fspath t k) (key_to_fspath t k')
  end

  module U = Zarr.Util

  let create ?(perm=0o700) dirname =
    U.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    FS.{dirname = U.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname =
    if Sys.is_directory dirname
    then FS.{dirname = U.sanitize_dir dirname; perm}
    else raise (Zarr.Storage.Not_a_filesystem_store dirname)

  include Zarr.Storage.Make(FS)
end
