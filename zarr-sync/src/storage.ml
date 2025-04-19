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
    type t = {dirname : string; perm : int}
    type error = [ `Read of string | `Write of string ]

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos (String.length path - pos)

    let key_to_fspath t key = Filename.concat t.dirname key

    let get t key = match In_channel.(with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) input_all) with
      | exception Sys_error _ -> Error (`Zarr (`Read (Format.sprintf "%s not found" key)))
      | x -> Ok x

    let get_partial_values t key ranges =
      let read_range ~none ~ic ~size (range_start, len) acc =
        In_channel.seek ic (Int64.of_int range_start);
        let char_length = Option.fold ~none:(size - range_start) ~some:Fun.id len in
        Result.bind
          (Option.to_result ~none (In_channel.really_input_string ic char_length))
          (fun a -> Result.map (List.cons a) acc)
      in
      In_channel.with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) @@ fun ic ->
      let size = Int64.to_int (In_channel.length ic) in
      let none = `Zarr (`Read "end of file reached during read.") in
      List.fold_right (read_range ~none ~ic ~size) ranges (Ok [])

    let set t key v =
      let p = key_to_fspath t key in
      Zarr.Util.create_parent_dir p t.perm;
      let f = [Open_wronly; Open_trunc; Open_creat] in
      Out_channel.(with_open_gen f t.perm p @@ fun oc -> output_string oc v; Ok (flush oc))

    let set_partial_values t key ?(append=false) rvs =
      let write ~oc (rs, value) =
        Out_channel.seek oc (Int64.of_int rs);
        Out_channel.output_string oc value
      in
      let p = key_to_fspath t key in
      Zarr.Util.create_parent_dir p t.perm;
      let flags = match append with
        | false -> [Open_creat; Open_wronly]
        | true -> [Open_append; Open_creat; Open_wronly]
      in
      Out_channel.with_open_gen flags t.perm p @@ fun oc ->
      List.iter (write ~oc) rvs;
      Ok (Out_channel.flush oc)

    let size t key = match In_channel.(with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) length) with
      | exception Sys_error _ -> Ok 0
      | x -> Ok (Int64.to_int x)
    
    let list_dir t prefix =
      let choose ~t ~dir x = match Filename.concat dir x with
        | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let dir = key_to_fspath t prefix in
      let dir_contents = Array.to_list (Sys.readdir dir) in
      Ok (List.partition_map (choose ~t ~dir) dir_contents)

    let rec walk t acc dir =
      let accumulate ~t a x = match Filename.concat dir x with
        | p when Sys.is_directory p -> walk t a p
        | p -> Result.map (List.cons (fspath_to_key t p)) a
      in
      Array.fold_left (accumulate ~t) acc (Sys.readdir dir)

    let list_prefix t prefix = walk t (Ok []) (key_to_fspath t prefix)
    let list t = walk t (Ok []) (key_to_fspath t "")
    let is_member t key = Ok (Sys.file_exists (key_to_fspath t key))
    let erase t key = Ok (Sys.remove (key_to_fspath t key))
    let rename t k k' = Ok (Sys.rename (key_to_fspath t k) (key_to_fspath t k'))

    let erase_prefix t pre =
      let maybe_delete acc x = Result.bind acc (fun () -> erase t x) in
      let batch_delete = List.fold_left maybe_delete (Ok ()) in
      Result.bind (list_prefix t pre) batch_delete
  end

  let create ?(perm=0o700) dirname =
    Zarr.Util.create_parent_dir dirname perm;
    match Sys.mkdir dirname perm with
    | exception Sys_error msg -> Error (`Zarr (`Write msg))
    | () -> Ok S.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname = match Sys.is_directory dirname with
    | exception Sys_error msg -> Error (`Zarr (`Read msg))
    | false -> Error (`Zarr (`Read (Format.sprintf "%s is not a directory." dirname)))
    | true -> Ok S.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  include Zarr.Storage.Make(IO)(S)
end
