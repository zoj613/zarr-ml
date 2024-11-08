module ZipStore = Zarr.Zip.Make(Deferred)
module MemoryStore = Zarr.Memory.Make(Deferred)

module FilesystemStore = struct
  module IO = struct
    module Deferred = Deferred

    type t = {root : Eio.Fs.dir_ty Eio.Path.t; perm : Eio.File.Unix_perm.t}

    let fspath_to_key t (path : Eio.Fs.dir_ty Eio.Path.t) =
      let s = snd path in
      let pos = String.length (snd t.root) + 1 in
      String.sub s pos (String.length s - pos)

    let key_to_fspath t key = Eio.Path.(t.root / key)

    let size t key =
      let flow_size flow = Optint.Int63.to_int (Eio.File.size flow) in
      try Eio.Path.with_open_in (key_to_fspath t key) flow_size with
      | Eio.Io (Eio.Fs.E Not_found Eio_unix.Unix_error _, _) -> 0

    let get t key =
      try Eio.Path.load (key_to_fspath t key) with
      | Eio.Io (Eio.Fs.E Not_found Eio_unix.Unix_error _, _) ->
        raise (Zarr.Storage.Key_not_found key)

    let get_partial_values t key ranges =
      let add ~size a (s, l) =
        let a' = Option.fold ~none:(a + size - s) ~some:(Int.add a) l in
        a', (Optint.Int63.of_int s, a, a' - a)
      in
      let read ~flow ~buffer (file_offset, off, len) =
        let _ = Eio.File.seek flow file_offset `Set in
        let buf = Cstruct.of_bigarray ~off ~len buffer in
        Eio.File.pread_exact flow ~file_offset [buf];
        Cstruct.to_string buf
      in
      Eio.Path.with_open_in (key_to_fspath t key) @@ fun flow ->
      let size = Optint.Int63.to_int (Eio.File.size flow) in
      let size', ranges' = List.fold_left_map (add ~size) 0 ranges in
      let buffer = Bigarray.Array1.create Char C_layout size' in
      List.map (read ~flow ~buffer) ranges'

    let create_parent_dir fp perm =
      Option.fold
        ~some:(fun (p, _) -> Eio.Path.mkdirs ~exists_ok:true ~perm p)
        ~none:()
        (Eio.Path.split fp)

    let set t key value =
      let fp = key_to_fspath t key in
      create_parent_dir fp t.perm;
      Eio.Path.save ~create:(`Or_truncate t.perm) fp value

    let set_partial_values t key ?(append=false) rvs =
      let write = if append then
        fun ~flow ~allocator (_, str) ->
        Eio.File.pwrite_all flow ~file_offset:Optint.Int63.max_int [Cstruct.of_string ~allocator str]
      else
        fun ~flow ~allocator (ofs, str) ->
        let file_offset = Eio.File.seek flow (Optint.Int63.of_int ofs) `Set in
        Eio.File.pwrite_all flow ~file_offset [Cstruct.of_string ~allocator str]
      in
      let l = List.fold_left (fun a (_, s) -> Int.max a (String.length s)) 0 rvs in
      let buffer = Bigarray.Array1.create Char C_layout l in
      let allocator len = Cstruct.of_bigarray ~off:0 ~len buffer in
      let fp = key_to_fspath t key in
      create_parent_dir fp t.perm;
      Eio.Path.with_open_out ~append ~create:(`If_missing t.perm) fp @@ fun flow ->
      List.iter (write ~flow ~allocator) rvs

    let rec walk t acc dir =
      let add ~t ~dir a x = match Eio.Path.(dir / x) with 
        | p when Eio.Path.is_directory p -> walk t a p
        | p -> (fspath_to_key t p) :: a
      in
      List.fold_left (add ~t ~dir) acc (Eio.Path.read_dir dir)

    let list t = walk t [] t.root

    let list_prefix t prefix = walk t [] (key_to_fspath t prefix)

    let is_member t key = Eio.Path.is_file (key_to_fspath t key)

    let erase t key = Eio.Path.unlink (key_to_fspath t key)

    let erase_prefix t pre =
      (* if prefix points to the root of the store, only delete sub-dirs and files.*)
      let prefix = key_to_fspath t pre in
      if Filename.chop_suffix (snd prefix) "/" = snd t.root
      then Eio.Fiber.List.iter (erase t) (list_prefix t pre)
      else Eio.Path.rmtree ~missing_ok:true prefix

    let list_dir t prefix =
      let choose ~t ~dir x = match Eio.Path.(dir / x) with
        | p when Eio.Path.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let dir = key_to_fspath t prefix in
      List.partition_map (choose ~t ~dir) (Eio.Path.read_dir dir)

    let rename t k k' = Eio.Path.rename (key_to_fspath t k) (key_to_fspath t k')
  end

  let create ?(perm=0o700) ~env dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    IO.{root = Eio.Path.(Eio.Stdenv.fs env / Zarr.Util.sanitize_dir dirname); perm}

  let open_store ?(perm=0o700) ~env dirname =
    if Sys.is_directory dirname
    then IO.{root = Eio.Path.(Eio.Stdenv.fs env / Zarr.Util.sanitize_dir dirname); perm}
    else raise (Zarr.Storage.Not_a_filesystem_store dirname)

  include Zarr.Storage.Make(IO)
end
