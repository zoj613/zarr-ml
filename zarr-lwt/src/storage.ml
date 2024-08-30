module MemoryStore = struct
  include Zarr.Storage.Make(Zarr.Memory.Make(Deferred))
  let create = Zarr.Memory.create
end

module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred
    open Deferred.Infix

    type t = {dirname : string; perm : Lwt_unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos @@ String.length path - pos

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let parent_dir = Filename.dirname fn in
      Lwt_unix.file_exists parent_dir >>= function
      | false ->
        create_parent_dir parent_dir perm >>= fun () ->
        Lwt_unix.mkdir parent_dir perm
      | true -> Lwt.return_unit

    let size t key =
      Lwt_io.file_length (key_to_fspath t key) >>| Int64.to_int

    let get t key =
      size t key >>= fun bufsize ->
      Lwt.catch
        (fun () ->
          Lwt_io.with_file
            ~buffer:(Lwt_bytes.create bufsize)
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
      size t key >>= fun tot ->
      let l =
        List.fold_left
          (fun a (s, l) ->
            Option.fold
              ~none:(Int.max a (tot - s)) ~some:(Int.max a) l) 0 ranges in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create l)
        ~flags:Unix.[O_RDONLY; O_NONBLOCK]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        (fun ic ->
          Lwt_list.map_s
            (fun (ofs, len) -> 
              let count = Option.fold ~none:(tot - ofs) ~some:Fun.id len in
              Lwt_io.set_position ic @@ Int64.of_int ofs >>= fun () ->
              Lwt_io.read ~count ic) ranges)

    let set t key value =
      let filename = key_to_fspath t key in
      create_parent_dir filename t.perm >>= fun () ->
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
        (fun oc ->
          Lwt_list.iter_s
            (fun (ofs, value) ->
              Lwt_io.set_position oc @@ Int64.of_int ofs >>= fun () ->
              Lwt_io.write oc value) rvs)

    let list t =
      let rec filter_concat acc dir =
        Lwt_stream.fold_s
          (fun x a -> 
            if x = "." || x  = ".." then Lwt.return a else
            match Filename.concat dir x with
            | p when Sys.is_directory p -> filter_concat a p
            | p -> Lwt.return @@ fspath_to_key t p :: a)
          (Lwt_unix.files_of_directory dir) acc
      in filter_concat [] @@ key_to_fspath t ""

    let is_member t key =
      Lwt_unix.file_exists @@ key_to_fspath t key

    let erase t key =
      Lwt_unix.unlink @@ key_to_fspath t key

    let list_prefix t prefix =
      let rec filter_concat acc dir =
        Lwt_stream.fold_s
          (fun x a -> 
            if x = "." || x  = ".." then Lwt.return a else
            match Filename.concat dir x with
            | p when Sys.is_directory p -> filter_concat a p
            | p ->
              let key = fspath_to_key t p in 
              if String.starts_with ~prefix key
              then Lwt.return @@ key :: a else Lwt.return a)
          (Lwt_unix.files_of_directory dir) acc
      in filter_concat [] @@ key_to_fspath t ""

    let erase_prefix t pre =
      list_prefix t pre >>= Lwt_list.iter_s @@ erase t

    let list_dir t prefix =
      let module S = Zarr.Util.StrSet in
      let n = String.length prefix in
      let rec filter_concat acc dir =
        Lwt_stream.fold_s
          (fun x ((l, r) as a) -> 
            if x = "." || x  = ".." then Lwt.return a else
            match Filename.concat dir x with
            | p when Sys.is_directory p -> filter_concat a p
            | p ->
              let key = fspath_to_key t p in
              let pred = String.starts_with ~prefix key in
              match key with
              | k when pred && String.contains_from k n '/' ->
                Lwt.return (S.add String.(sub k 0 @@ 1 + index_from k n '/') l, r)
              | k when pred -> Lwt.return (l, k :: r)
              | _ -> Lwt.return a)
          (Lwt_unix.files_of_directory dir) acc
      in
      filter_concat (S.empty, []) (key_to_fspath t "") >>| fun (y, x) ->
      x, S.elements y
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
