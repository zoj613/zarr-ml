module IO = struct
  type 'a t = 'a
  let return = Result.ok
  let error = Result.error
  let return_unit = Ok ()
  let lift = Fun.id
  let bind = Result.bind
  let map = Result.map
  let rec fold_left f acc xs = match xs with
    | [] -> lift acc
    | x :: l -> bind (f acc x) (fun k -> fold_left f (Ok k) l)

  module Infix = struct
    let (>>=) = bind
    let (>>|) x f = map f x
  end

  module Syntax = struct
    let (let*) = Infix.(>>=)
    let (let+) = Infix.(>>|)
  end
end

module ZipStore = Zarr.Zip.Make(IO)
module MemoryStore = Zarr.Memory.Make(IO)

module FilesystemStore = struct
  module S = struct
    type 'a io = 'a
    type t = {root : Eio.Fs.dir_ty Eio.Path.t; perm : Eio.File.Unix_perm.t}
    type error = [ `Read of string | `Write of string ]

    let fspath_to_key t (path : Eio.Fs.dir_ty Eio.Path.t) =
      let s = snd path and pos = String.length (snd t.root) + 1 in
      String.sub s pos (String.length s - pos)

    let key_to_fspath t key = Eio.Path.(t.root / key)

    let size t key =
      let flow_size flow = Optint.Int63.to_int (Eio.File.size flow) in
      match Eio.Path.with_open_in (key_to_fspath t key) flow_size with
      | exception Eio.Io (Eio.Fs.E Not_found Eio_unix.Unix_error _, _) -> Ok 0
      | x -> Ok x

    let get t key = match Eio.Path.load (key_to_fspath t key) with
      | exception Eio.Io (Eio.Fs.E Not_found Eio_unix.Unix_error _, _) ->
        Error (`Zarr (`Read (Format.sprintf "%s not found" key)))
      | x -> Ok x

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
      Ok (List.map (read ~flow ~buffer) ranges')

    let create_parent_dir fp perm =
      Option.fold
        ~some:(fun (p, _) -> Eio.Path.mkdirs ~exists_ok:true ~perm p)
        ~none:()
        (Eio.Path.split fp)

    let set t key value =
      let fp = key_to_fspath t key in
      create_parent_dir fp t.perm;
      Ok (Eio.Path.save ~create:(`Or_truncate t.perm) fp value)

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
      Ok (List.iter (write ~flow ~allocator) rvs)

    let rec walk t acc dir =
      let add ~t ~dir a x = match Eio.Path.(dir / x) with 
        | p when Eio.Path.is_directory p -> walk t a p
        | p -> Result.map (List.cons (fspath_to_key t p)) a
      in
      List.fold_left (add ~t ~dir) acc (Eio.Path.read_dir dir)

    let list t = walk t (Ok []) t.root
    let list_prefix t prefix = walk t (Ok []) (key_to_fspath t prefix)
    let is_member t key = Ok (Eio.Path.is_file (key_to_fspath t key))
    let erase t key = Ok (Eio.Path.unlink (key_to_fspath t key))
    let rename t k k' = Ok (Eio.Path.rename (key_to_fspath t k) (key_to_fspath t k'))

    let erase_prefix t pre =
      (* if prefix points to the root of the store, only delete sub-dirs and files.*)
      let open Zarr.Util.Result_syntax in
      let maybe_delete acc x = Result.bind acc (fun () -> erase t x) in
      let batch_delete = List.fold_left maybe_delete (Ok ()) in
      let prefix = key_to_fspath t pre in
      let prefix_path = snd prefix in
      let none = `Zarr (`Read (Format.sprintf "%s not found" prefix_path)) in
      let* path = Option.to_result ~none (Filename.chop_suffix_opt ~suffix:"/" prefix_path) in
      if path = snd t.root then Result.bind (list_prefix t pre) batch_delete else
      Ok (Eio.Path.rmtree ~missing_ok:true prefix)

    let list_dir t prefix =
      let choose ~t ~dir x = match Eio.Path.(dir / x) with
        | p when Eio.Path.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let dir = key_to_fspath t prefix in
      Ok (List.partition_map (choose ~t ~dir) (Eio.Path.read_dir dir))
  end

  let create ?(perm=0o700) ~env dirname =
    Zarr.Util.create_parent_dir dirname perm;
    match Sys.mkdir dirname perm with
    | exception Sys_error msg -> Error (`Zarr (`Write msg))
    | () -> Ok S.{root = Eio.Path.(Eio.Stdenv.fs env / Zarr.Util.sanitize_dir dirname); perm}

  let open_store ?(perm=0o700) ~env dirname = match Sys.is_directory dirname with
    | exception Sys_error msg -> Error (`Zarr (`Read msg))
    | false -> Error (`Zarr (`Read (Format.sprintf "%s is not a directory." dirname)))
    | true -> Ok S.{root = Eio.Path.(Eio.Stdenv.fs env / Zarr.Util.sanitize_dir dirname); perm}

  include Zarr.Storage.Make(IO)(S)
end
