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
      let ld = String.length t.dirname in
      let pos =
        if Filename.dir_sep = String.sub t.dirname (ld - 1) 1
        then ld else ld + 1 in
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
        | exn -> Lwt.reraise exn)

    let get_partial_values t key ranges =
      size t key >>= fun bufsize ->
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create bufsize)
        ~flags:Unix.[O_RDONLY; O_NONBLOCK]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        (fun ic ->
          Lwt_io.length ic >>= fun v ->
          let size = Int64.to_int v in
          Lwt_list.map_s
            (fun (rs, len) -> 
              let count =
                match len with
                | Some l -> l
                | None -> size - rs in
              Lwt_io.set_position ic @@ Int64.of_int rs >>= fun () ->
              Lwt_io.read ~count ic) ranges)

    let set t key value =
      let filename = key_to_fspath t key in
      create_parent_dir filename t.perm >>= fun () ->
      Lwt_io.with_file
        ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT; O_NONBLOCK]
        ~perm:t.perm
        ~mode:Lwt_io.Output
        filename
        (Fun.flip Lwt_io.write value)

    let set_partial_values t key ?(append=false) rvs =
      size t key >>= fun bufsize ->
      let flags = Unix.[O_NONBLOCK; O_WRONLY] in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create bufsize)
        ~flags:(if append then Unix.O_APPEND :: flags else flags)
        ~perm:t.perm
        ~mode:Lwt_io.Output
        (key_to_fspath t key)
        (fun oc ->
          Lwt_list.iter_s
            (fun (rs, value) ->
              Lwt_io.set_position oc @@ Int64.of_int rs >>= fun () ->
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

    let list_prefix t pre =
      list t >>= Lwt_list.filter_s
        (fun x -> Lwt.return @@ String.starts_with ~prefix:pre x)

    let erase_prefix t pre =
      list_prefix t pre >>= Lwt_list.iter_s @@ erase t

    let list_dir t pre =
      let module StrSet = Zarr.Util.StrSet in
      let n = String.length pre in
      list_prefix t pre >>= fun pk ->
      Lwt_list.partition_s
        (fun k -> Lwt.return @@ String.contains_from k n '/') pk
      >>= fun (other, keys) ->
      Lwt_list.map_s
        (fun k ->
          Lwt.return @@ String.sub k 0 @@ 1 + String.index_from k n '/') other
      >>| fun prefixes -> keys, StrSet.(of_list prefixes |> elements)
  end

  let create ?(perm=0o700) dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    FS.{dirname; perm}

  let open_store ?(perm=0o700) dirname =
    if Sys.is_directory dirname then FS.{dirname; perm}
    else raise @@ Zarr.Storage.Not_a_filesystem_store dirname

  include Zarr.Storage.Make(FS)
end
