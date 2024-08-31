module MemoryStore = struct
  include Zarr.Storage.Make(Zarr.Memory.Make(Deferred))
  let create = Zarr.Memory.create
end

module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred

    type t = {dirname : string; perm : Unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos @@ String.length path - pos

    let key_to_fspath t key = Filename.concat t.dirname key

    let get t key =
      try
        In_channel.with_open_gen
          In_channel.[Open_rdonly; Open_nonblock]
          t.perm
          (key_to_fspath t key)
          In_channel.input_all
      with
      | Sys_error _ -> raise @@ Zarr.Storage.Key_not_found key

    let get_partial_values t key ranges =
      In_channel.with_open_gen
        In_channel.[Open_rdonly; Open_nonblock]
        t.perm
        (key_to_fspath t key)
        (fun ic ->
          let size = In_channel.length ic |> Int64.to_int in
          List.map
            (fun (ofs, len) ->
              In_channel.seek ic @@ Int64.of_int ofs;
              let l = Option.fold ~none:(size - ofs) ~some:Fun.id len in 
              Option.get @@ In_channel.really_input_string ic l) ranges)

    let set t key value =
      let filename = key_to_fspath t key in
      Zarr.Util.create_parent_dir filename t.perm;
      Out_channel.with_open_gen
        Out_channel.[Open_wronly; Open_trunc; Open_creat; Open_nonblock]
        t.perm
        filename
        (fun oc -> Out_channel.output_string oc value; Out_channel.flush oc)

    let set_partial_values t key ?(append=false) rvs =
      let open Out_channel in
      Out_channel.with_open_gen
        [Open_nonblock; if append then Open_append else Open_wronly]
        t.perm
        (key_to_fspath t key)
        (fun oc ->
          List.iter
            (fun (rs, value) ->
              Out_channel.seek oc @@ Int64.of_int rs;
              Out_channel.output_string oc value) rvs; Out_channel.flush oc)

    let is_member t key = Sys.file_exists @@ key_to_fspath t key

    let erase t key = Sys.remove @@ key_to_fspath t key

    let size t key =
      In_channel.with_open_gen
        In_channel.[Open_rdonly; Open_nonblock]
        t.perm
        (key_to_fspath t key)
        (fun ic -> In_channel.length ic |> Int64.to_int)

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
      let module StrSet = Zarr.Util.StrSet in
      let n = String.length prefix in
      let rec aux acc dir =
        let xs = Sys.readdir dir in
        List.fold_left
          (fun ((l, r) as a) x ->
            match Filename.concat dir x with
            | p when Sys.is_directory p -> aux a p
            | p ->
              let key = fspath_to_key t p in
              let pred = String.starts_with ~prefix key in
              match key with
              | k when pred && String.contains_from k n '/' ->
                StrSet.add String.(sub k 0 @@ 1 + index_from k n '/') l, r
              | k when pred -> l, k :: r
              | _ -> a) acc (Array.to_list xs)
        in
        let prefs, keys = aux (StrSet.empty, []) @@ key_to_fspath t "" in
        keys, StrSet.elements prefs
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
