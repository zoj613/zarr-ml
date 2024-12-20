module Deferred = struct
  type 'a t = 'a
  let return = Fun.id
  let return_unit = ()
  let iter = List.iter
  let fold_left = List.fold_left
  let concat_map = List.concat_map

  module Infix = struct
    let (>>=) x f = f x
    let (>>|) = (>>=)
  end

  module Syntax = struct
    let (let*) x f = f x
    let (let+) = (let*)
  end
end

module ZipStore = Zarr.Zip.Make(Deferred)
module MemoryStore = Zarr.Memory.Make(Deferred)

module FilesystemStore = struct
  module IO = struct
    module Deferred = Deferred

    type t = {dirname : string; perm : Unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos (String.length path - pos)

    let key_to_fspath t key = Filename.concat t.dirname key

    let get t key =
      let p = key_to_fspath t key in
      try In_channel.(with_open_gen [Open_rdonly] t.perm p input_all) with
      | Sys_error _ -> raise (Zarr.Storage.Key_not_found key)

    let get_partial_values t key ranges =
      let read_range ~ic ~size (ofs, len) =
        In_channel.seek ic (Int64.of_int ofs);
        match len with
        | None -> really_input_string ic (size - ofs)
        | Some rs -> really_input_string ic rs
      in
      In_channel.with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) @@ fun ic ->
      let size = Int64.to_int (In_channel.length ic) in
      List.map (read_range ~ic ~size) ranges

    let set t key v =
      let p = key_to_fspath t key in
      Zarr.Util.create_parent_dir p t.perm;
      let f = [Open_wronly; Open_trunc; Open_creat] in
      Out_channel.(with_open_gen f t.perm p @@ fun oc -> output_string oc v; flush oc)

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
      Out_channel.flush oc

    let is_member t key = Sys.file_exists (key_to_fspath t key)

    let erase t key = Sys.remove (key_to_fspath t key)

    let size t key =
      match In_channel.(with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) length) with
      | exception Sys_error _ -> 0
      | s -> Int64.to_int s

    let rec walk t acc dir =
      let accumulate ~t a x = match Filename.concat dir x with
        | p when Sys.is_directory p -> walk t a p
        | p -> (fspath_to_key t p) :: a
      in
      let dir_contents = Array.to_list (Sys.readdir dir) in
      List.fold_left (accumulate ~t) acc dir_contents
    
    let list t = walk t [] (key_to_fspath t "")

    let list_prefix t prefix = walk t [] (key_to_fspath t prefix)

    let erase_prefix t pre = List.iter (erase t) (list_prefix t pre)

    let list_dir t prefix =
      let choose ~t ~dir x = match Filename.concat dir x with
        | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let dir = key_to_fspath t prefix in
      let dir_contents = Array.to_list (Sys.readdir dir) in
      List.partition_map (choose ~t ~dir) dir_contents

    let rename t k k' = Sys.rename (key_to_fspath t k) (key_to_fspath t k')
  end

  let create ?(perm=0o700) dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    IO.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname =
    if Sys.is_directory dirname
    then IO.{dirname = Zarr.Util.sanitize_dir dirname; perm}
    else raise (Zarr.Storage.Not_a_filesystem_store dirname)

  include Zarr.Storage.Make(IO)
end
