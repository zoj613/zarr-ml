module IO = struct
  type 'a t = 'a
  let return = Fun.id
  let bind x f = f x
  let map f x = f x
  let return_unit = ()
  let iter f xs = Eio.Fiber.List.iter f xs
  let fold_left = List.fold_left
  let concat_map f xs = List.concat (Eio.Fiber.List.map f xs)

  module Infix = struct
    let (>>=) = bind
    let (>>|) = (>>=)
  end

  module Syntax = struct
    let (let*) = bind
    let (let+) = (let*)
  end
end

module ZipStore = Zarr.Zip.Make(IO)
module MemoryStore = Zarr.Memory.Make(IO)

module FilesystemStore = struct
  module S = struct
    type t = {root : Eio.Fs.dir_ty Eio.Path.t; perm : Eio.File.Unix_perm.t}
    type 'a io = 'a IO.t

    let fspath_to_key t (path : Eio.Fs.dir_ty Eio.Path.t) =
      let s = snd path and pos = String.length (snd t.root) + 1 in
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
    let rename t k k' = Eio.Path.rename (key_to_fspath t k) (key_to_fspath t k')

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
  end

  let create ?(perm=0o700) ~env dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    S.{root = Eio.Path.(Eio.Stdenv.fs env / Zarr.Util.sanitize_dir dirname); perm}

  let open_store ?(perm=0o700) ~env dirname =
    if Sys.is_directory dirname
    then S.{root = Eio.Path.(Eio.Stdenv.fs env / Zarr.Util.sanitize_dir dirname); perm}
    else raise (Zarr.Storage.Not_a_filesystem_store dirname)

  include Zarr.Storage.Make(IO)(S)
end

module HttpStore = struct
  exception Not_implemented
  exception Request_failed of int * string

  open Cohttp_eio

  let raise_status_error e =
    let c = Cohttp.Code.code_of_status e in
    raise (Request_failed (c, Cohttp.Code.reason_phrase_of_code c))

  let fold_response ~success resp key = match Http.Response.status resp with
    | #Http.Status.success -> success ()
    | #Http.Status.client_error as e when e = `Not_found ->
      raise (Zarr.Storage.Key_not_found key)
    | e -> raise_status_error e

  module IO = struct
    module Deferred = Deferred

    type t = {base_url : Uri.t; client : Client.t}

    let get t key =
      Eio.Switch.run @@ fun sw ->
      let url = Uri.with_path t.base_url key in
      let resp, body = Client.get ~sw t.client url in
      fold_response ~success:(fun () -> Eio.Flow.read_all body) resp key

    let size t key = try String.length (get t key) with
      | Zarr.Storage.Key_not_found _ -> 0
      
    (*let size t key =
      let content_length resp () = match Http.Response.content_length resp with
        | Some l -> l
        | None -> String.length (get t key)
      in
      Eio.Switch.run @@ fun sw ->
      let url = Uri.with_path t.base_url key in
      let resp = Client.head ~sw t.client url in
      fold_response ~success:(content_length resp) resp key *)

    let is_member t key = if (size t key) > 0 then true else false

    let get_partial_values t key ranges =
      let read_range ~data ~size (ofs, len) = match len with
        | None -> String.sub data ofs (size - ofs)
        | Some l -> String.sub data ofs l
      in
      let data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let set t key data =
      Eio.Switch.run @@ fun sw ->
      let url = Uri.with_path t.base_url key in
      let headers = Http.Header.of_list [("Content-Length", string_of_int (String.length data))] in
      let body = Body.of_string data in
      let resp, _ = Client.put ~sw ~headers ~body t.client url in
      fold_response ~success:(fun () -> ()) resp key
 
    let set_partial_values t key ?(append=false) rsv =
      let ov = try get t key with
        | Zarr.Storage.Key_not_found _ -> String.empty
      in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      set t key (List.fold_left f ov rsv)
    
    (*let erase t key =
      Eio.Switch.run @@ fun sw ->
      let url = Uri.with_path t.base_url key in
      let resp, _ = Client.delete ~sw t.client url in
      match Http.Response.status resp with
      | #Http.Status.success -> Deferred.return_unit
      | #Http.Status.client_error as e when e = `Not_found -> Deferred.return_unit
      | e -> raise_status_error e *)

    let erase _ = raise Not_implemented
    let erase_prefix _ = raise Not_implemented
    let list _ = raise Not_implemented
    let list_dir _ = raise Not_implemented
    let rename _ = raise Not_implemented
  end

  let with_open ~net uri f =
    let client = Client.make ~https:None net in
    f IO.{client; base_url = uri}

  include Zarr.Storage.Make(IO)
end
