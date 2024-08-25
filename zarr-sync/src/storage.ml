module MemoryStore = struct
  include Zarr.Storage.Make(Zarr.Memory.Make(Deferred))
  let create = Zarr.Memory.create
end

module FilesystemStore = struct
  module FS = struct
    module Deferred = Deferred
    open Deferred.Infix

    type t = {dirname : string; perm : Unix.file_perm}

    let fspath_to_key t path =
      let ld = String.length t.dirname in
      let pos =
        if Filename.dir_sep = String.sub t.dirname (ld - 1) 1
        then ld else ld + 1 in
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
            (fun (rs, len) ->
              let len' =
                match len with
                | Some l -> l
                | None -> size - rs in
              In_channel.seek ic @@ Int64.of_int rs;
              Option.get @@ In_channel.really_input_string ic len') ranges)

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

    let list t =
      let rec aux acc dir =
        match Sys.readdir dir with
        | [||] -> acc
        | xs ->
          List.concat_map
            (fun x ->
              match Filename.concat dir x with
              | p when Sys.is_directory p -> aux acc p
              | p -> (fspath_to_key t p) :: acc) @@ Array.to_list xs
      in aux [] @@ key_to_fspath t ""

    let is_member t key = Sys.file_exists @@ key_to_fspath t key

    let erase t key = Sys.remove @@ key_to_fspath t key

    let size t key =
      In_channel.with_open_gen
        In_channel.[Open_rdonly; Open_nonblock]
        t.perm
        (key_to_fspath t key)
        (fun ic -> In_channel.length ic |> Int64.to_int)

    let list_prefix t pre =
      List.filter (String.starts_with ~prefix:pre) (list t)

    let erase_prefix t pre =
      list_prefix t pre >>| List.iter @@ erase t

    let list_dir t pre =
      let module StrSet = Zarr.Util.StrSet in
      let n = String.length pre in
      list_prefix t pre >>| fun pk ->
      let prefixes, keys =
        List.partition_map
          (fun k ->
            if String.contains_from k n '/' then
              Either.left @@
              String.sub k 0 @@ 1 + String.index_from k n '/'
            else Either.right k) pk in
      keys, StrSet.(of_list prefixes |> elements)
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
