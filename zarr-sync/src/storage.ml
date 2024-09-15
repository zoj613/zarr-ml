module MemoryStore = struct
  include Zarr.Storage.Make(Zarr.Memory.Make(Deferred))
  let create = Zarr.Memory.create
end

module FilesystemStore = struct
  module F = struct
    module Deferred = Deferred

    type t = {dirname : string; perm : Unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos @@ String.length path - pos

    let key_to_fspath t key = Filename.concat t.dirname key

    let get t key =
      let p = key_to_fspath t key in
      try In_channel.(with_open_gen [Open_rdonly; Open_nonblock] t.perm p input_all)
      with Sys_error _ -> raise @@ Zarr.Storage.Key_not_found key

    let get_partial_values t key ranges =
      let f = [Open_rdonly; Open_nonblock] in
      In_channel.with_open_gen f t.perm (key_to_fspath t key) @@ fun ic ->
      let s = In_channel.length ic |> Int64.to_int in
      ranges |> List.map @@ fun (ofs, len) ->
      In_channel.seek ic @@ Int64.of_int ofs;
      let l = Option.fold ~none:(s - ofs) ~some:Fun.id len in 
      Option.get @@ In_channel.really_input_string ic l

    let set t key v =
      let p = key_to_fspath t key in
      Zarr.Util.create_parent_dir p t.perm;
      let f = [Open_wronly; Open_trunc; Open_creat; Open_nonblock] in
      Out_channel.(with_open_gen f t.perm p @@ fun oc -> output_string oc v; flush oc)

    let set_partial_values t key ?(append=false) rvs =
      let f = [Open_nonblock; if append then Open_append else Open_wronly] in
      let p = key_to_fspath t key in
      Out_channel.with_open_gen f t.perm p @@ fun oc ->
      rvs |> List.iter (fun (rs, value) ->
      Out_channel.seek oc @@ Int64.of_int rs;
      Out_channel.output_string oc value);
      Out_channel.flush oc

    let is_member t key = Sys.file_exists @@ key_to_fspath t key

    let erase t key = Sys.remove @@ key_to_fspath t key

    let size t key =
      let f = [Open_rdonly; Open_nonblock] in
      In_channel.(with_open_gen f t.perm (key_to_fspath t key) length) |> Int64.to_int

    let rec walk t acc dir =
      List.fold_left
        (fun a x ->
          match Filename.concat dir x with
          | p when Sys.is_directory p -> walk t a p
          | p -> (fspath_to_key t p) :: a) acc (Array.to_list @@ Sys.readdir dir)
    
    let list t = walk t [] (key_to_fspath t "")

    let list_prefix t prefix =
      walk t [] (key_to_fspath t prefix)

    let erase_prefix t pre =
      List.iter (erase t) @@ list_prefix t pre

    let list_dir t prefix =
      let dir = key_to_fspath t prefix in
      (Array.to_list @@ Sys.readdir dir) |> List.partition_map @@ fun x ->
      match Filename.concat dir x with
      | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
      | p -> Either.left @@ fspath_to_key t p 

    let rename t k k' = Sys.rename (key_to_fspath t k) (key_to_fspath t k')
  end

  module U = Zarr.Util

  let create ?(perm=0o700) dirname =
    U.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    F.{dirname = U.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname =
    if Sys.is_directory dirname
    then F.{dirname = U.sanitize_dir dirname; perm}
    else raise @@ Zarr.Storage.Not_a_filesystem_store dirname

  include Zarr.Storage.Make(F)
end
