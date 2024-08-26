module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred

    type t = {root : Eio.Fs.dir_ty Eio.Path.t; perm : Eio.File.Unix_perm.t}

    let fspath_to_key t (path : Eio.Fs.dir_ty Eio.Path.t) =
      let s = snd path in
      let pos = String.length (snd t.root) + 1 in
      String.sub s pos @@ String.length s - pos

    let key_to_fspath t key = Eio.Path.(t.root / key)

    let size t key =
      let fp = key_to_fspath t key in
      Eio.Path.with_open_in fp @@ fun flow ->
      Optint.Int63.to_int (Eio.File.size flow)

    let get t key =
      try Eio.Path.load @@ key_to_fspath t key with
      | Eio.Io (Eio.Fs.E Not_found Eio_unix.Unix_error _, _) ->
        raise (Zarr.Storage.Key_not_found key)

    let get_partial_values t key ranges =
      let fp = key_to_fspath t key in
      Eio.Path.with_open_in fp @@ fun flow ->
      let filesize = Optint.Int63.to_int (Eio.File.size flow) in
      ranges |> List.map @@ fun (start, len) ->
      let bufsize =
        match len with
        | Some l -> l
        | None -> filesize - start in
      let pos = Optint.Int63.of_int start in
      let file_offset = Eio.File.seek flow pos `Set in
      let buf = Cstruct.create bufsize in
      Eio.File.pread_exact flow ~file_offset [buf];
      Cstruct.to_string buf

    let set t key value =
      let fp = key_to_fspath t key in
      let parent_dir = fst @@ Option.get @@ Eio.Path.split fp in
      Eio.Path.mkdirs ~exists_ok:true ~perm:t.perm parent_dir;
      Eio.Path.save ~create:(`Or_truncate t.perm) fp value

    let set_partial_values t key ?(append=false) rvs =
      let fp = key_to_fspath t key in
      Eio.Path.with_open_out ~append ~create:`Never fp @@ fun flow ->
      List.iter
        (fun (start, value) ->
          let file_offset = Optint.Int63.of_int start in
          let _ = Eio.File.seek flow file_offset `Set in
          let buf = Cstruct.of_string value in
          Eio.File.pwrite_all flow ~file_offset [buf]) rvs

    let list t =
      let rec aux acc dir =
        match Eio.Path.read_dir dir with
        | [] -> acc
        | xs ->
          List.concat_map
            (fun x ->
              match Eio.Path.(dir / x) with 
              | p when Eio.Path.is_directory p -> aux acc p
              | p -> (fspath_to_key t p) :: acc) xs
      in aux [] t.root

    let is_member t key =
      Eio.Path.is_file @@ key_to_fspath t key

    let erase t key =
      Eio.Path.unlink @@ key_to_fspath t key

    let list_prefix t prefix =
      let rec aux acc dir =
        let xs = Eio.Path.read_dir dir in
        List.concat_map
          (fun x ->
            match Eio.Path.(dir / x) with 
            | p when Eio.Path.is_directory p -> aux acc p
            | p ->
              let key = fspath_to_key t p in
              if String.starts_with ~prefix key then key :: acc else acc) xs
      in aux [] t.root

    let erase_prefix t pre =
      (* if prefix points to the root of the store, only delete sub-dirs and files.*)
      let prefix = key_to_fspath t pre in
      if Filename.chop_suffix (snd prefix) "/" = snd t.root
      then List.iter (erase t) @@ list_prefix t pre
      else Eio.Path.rmtree ~missing_ok:true prefix

    let list_dir t prefix =
      let module StrSet = Zarr.Util.StrSet in
      let n = String.length prefix in
      let rec aux acc dir =
        List.fold_left
          (fun ((l, r) as a) x ->
            match Eio.Path.(dir / x) with 
            | p when Eio.Path.is_directory p -> aux a p
            | p ->
              let key = fspath_to_key t p in
              let pred = String.starts_with ~prefix key in
              match key with
              | k when pred && String.contains_from k n '/' ->
                StrSet.add String.(sub k 0 @@ 1 + index_from k n '/') l, r
              | k when pred -> l, k :: r
              | _ -> a) acc @@ Eio.Path.read_dir dir
      in
      let prefs, keys = aux (StrSet.empty, []) t.root in
      keys, StrSet.elements prefs
  end

  module U = Zarr.Util

  let create ?(perm=0o700) ~env dirname =
    U.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    FS.{root = Eio.Path.(Eio.Stdenv.fs env / U.sanitize_dir dirname); perm}

  let open_store ?(perm=0o700) ~env dirname =
    if Sys.is_directory dirname
    then FS.{root = Eio.Path.(Eio.Stdenv.fs env / U.sanitize_dir dirname); perm}
    else raise @@ Zarr.Storage.Not_a_filesystem_store dirname

  include Zarr.Storage.Make(FS)
end
