module MemoryStore = struct
  include Zarr.Storage.Make(Zarr.Memory.Make(Deferred))
  let create = Zarr.Memory.create
end

module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred

    type t = {root : Eio.Fs.dir_ty Eio.Path.t; perm : Eio.File.Unix_perm.t}

    let fspath_to_key t (path : Eio.Fs.dir_ty Eio.Path.t) =
      let s = snd path in
      let pos = String.length (snd t.root) + 1 in
      String.(sub s pos @@ length s - pos)

    let key_to_fspath t key = Eio.Path.(t.root / key)

    let size t key =
      Eio.Path.with_open_in (key_to_fspath t key) @@ fun flow ->
      Optint.Int63.to_int @@ Eio.File.size flow

    let get t key =
      try Eio.Path.load @@ key_to_fspath t key with
      | Eio.Io (Eio.Fs.E Not_found Eio_unix.Unix_error _, _) ->
        raise (Zarr.Storage.Key_not_found key)

    let get_partial_values t key ranges =
      Eio.Path.with_open_in (key_to_fspath t key) @@ fun flow ->
      let size = Optint.Int63.to_int @@ Eio.File.size flow in
      let size', ranges' =
        List.fold_left_map (fun a (s, l) ->
          let a' = Option.fold ~none:(a + size - s) ~some:(Int.add a) l in
          a', (Optint.Int63.of_int s, a, a' - a)) 0 ranges in
      let buffer = Bigarray.Array1.create Char C_layout size' in
      ranges' |> Eio.Fiber.List.map @@ fun (fo, off, len) ->
      let file_offset = Eio.File.seek flow fo `Set in
      let buf = Cstruct.of_bigarray ~off ~len buffer in
      Eio.File.pread_exact flow ~file_offset [buf];
      Cstruct.to_string buf

    let set t key value =
      let fp = key_to_fspath t key in
      Option.fold
        ~none:()
        ~some:(fun (p, _) -> Eio.Path.mkdirs ~exists_ok:true ~perm:t.perm p)
        (Eio.Path.split fp);
      Eio.Path.save ~create:(`Or_truncate t.perm) fp value

    let set_partial_values t key ?(append=false) rvs =
      let l = List.fold_left (fun a (_, s) -> Int.max a (String.length s)) 0 rvs in
      let buffer = Bigarray.Array1.create Char C_layout l in
      let allocator len = Cstruct.of_bigarray ~off:0 ~len buffer in
      Eio.Path.with_open_out ~append ~create:`Never (key_to_fspath t key) @@ fun flow ->
      rvs |> Eio.Fiber.List.iter @@ fun (ofs, str) ->
      let file_offset = Eio.File.seek flow (Optint.Int63.of_int ofs) `Set in
      Eio.File.pwrite_all flow ~file_offset [Cstruct.of_string ~allocator str]

    let rec walk t acc dir =
      List.fold_left
        (fun a x ->
          match Eio.Path.(dir / x) with 
          | p when Eio.Path.is_directory p -> walk t a p
          | p -> (fspath_to_key t p) :: a) acc (Eio.Path.read_dir dir)

    let list t = walk t [] t.root

    let list_prefix t prefix =
      walk t [] (key_to_fspath t prefix)

    let is_member t key =
      Eio.Path.is_file @@ key_to_fspath t key

    let erase t key =
      Eio.Path.unlink @@ key_to_fspath t key

    let erase_prefix t pre =
      (* if prefix points to the root of the store, only delete sub-dirs and files.*)
      let prefix = key_to_fspath t pre in
      if Filename.chop_suffix (snd prefix) "/" = snd t.root
      then Eio.Fiber.List.iter (erase t) @@ list_prefix t pre
      else Eio.Path.rmtree ~missing_ok:true prefix

    let list_dir t prefix =
      let module S = Zarr.Util.StrSet in
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
                S.add String.(sub k 0 @@ 1 + index_from k n '/') l, r
              | k when pred -> l, k :: r
              | _ -> a) acc (Eio.Path.read_dir dir) in
      let prefs, keys = aux (S.empty, []) t.root in
      keys, S.elements prefs
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
