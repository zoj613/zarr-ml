module Impl = struct
  type t =
    {dirname : string; file_perm : Unix.file_perm}

  let fspath_to_key t p =
    let ld = String.length t.dirname in
    String.sub p ld @@ String.length p - ld

  let key_to_fspath t key = t.dirname ^ key

  (* Obtained from:
    https://discuss.ocaml.org/t/how-to-create-a-new-file-while-automatically-creating-any-intermediate-directories/14837/5?u=zoj613 *)
  let rec create_parent_dir fn perm =
    let parent_dir = Filename.dirname fn in
    if not (Sys.file_exists parent_dir) then begin
      create_parent_dir parent_dir perm;
      Sys.mkdir parent_dir perm
    end

  let get t key =
    let fpath = key_to_fspath t key in
    try
      In_channel.with_open_gen
        In_channel.[Open_rdonly]
        t.file_perm
        fpath
        (fun ic -> Ok (In_channel.input_all ic))
    with
    | Sys_error _ -> Error (`Store_read fpath)

  let get_partial_values t key ranges =
    let open Util.Result_syntax in
    In_channel.with_open_gen
      In_channel.[Open_rdonly]
      t.file_perm
      (key_to_fspath t key)
      (fun ic ->
        let size = In_channel.length ic |> Int64.to_int in
        List.fold_right
          (fun (rs, len) acc ->
            acc >>= fun xs ->
            let len' =
              match len with
              | Some l -> l
              | None -> size - rs
            in
            In_channel.seek ic @@ Int64.of_int rs;
            match In_channel.really_input_string ic len' with
            | Some s -> Ok (s :: xs)
            | None ->
              Error (`Store_read "EOF reached before all bytes are read."))
          ranges (Ok []))

  let set t key value =
    let filename = key_to_fspath t key in
    create_parent_dir filename t.file_perm;
    Out_channel.with_open_gen
      Out_channel.[Open_wronly; Open_trunc; Open_creat]
      t.file_perm
      filename
      (fun oc -> Out_channel.output_string oc value; Out_channel.flush oc)

  let set_partial_values t key ?(append=false) rvs =
    let open Out_channel in
    Out_channel.with_open_gen
      [if append then Open_append else Open_wronly]
      t.file_perm
      (key_to_fspath t key)
      (fun oc ->
        List.iter
          (fun (rs, value) ->
            Out_channel.seek oc @@ Int64.of_int rs;
            Out_channel.output_string oc value) rvs; Out_channel.flush oc)

  let list t =
    let module StrSet = Storage_intf.Base.StrSet in
    let rec aux acc path =
      match Sys.readdir path with
      | [||] -> acc
      | xs ->
        Array.fold_left (fun set x ->
          match path ^ x with
          | p when Sys.is_directory p ->
            aux set @@ p ^ "/"
          | p ->
            StrSet.add (fspath_to_key t p) set) acc xs 
    in
  (* calling aux using the basepath t.dirname should not fail
     since this path already exists by virtue of being able to
     access this filestystem store. *)
    match
      StrSet.elements @@
      aux StrSet.empty @@
      key_to_fspath t "" 
    with
    | [] -> []
    | xs -> "" :: xs

  let is_member t key =
    Sys.file_exists @@ key_to_fspath t key

  let erase t key = 
    try Sys.remove @@ key_to_fspath t key with
    | Sys_error _ -> ()

  let size t key =
    In_channel.with_open_gen
      In_channel.[Open_rdonly]
      t.file_perm
      (key_to_fspath t key)
      (fun ic -> In_channel.length ic |> Int64.to_int)

  let erase_prefix t pre =
    Storage_intf.Base.erase_prefix
      ~list_fn:list ~erase_fn:erase t pre

  let list_prefix t pre =
    Storage_intf.Base.list_prefix ~list_fn:list t pre

  let list_dir t pre =
    Storage_intf.Base.list_dir ~list_fn:list t pre
end

let create ?(file_perm=0o700) path =
  Impl.create_parent_dir path file_perm;
  Sys.mkdir path file_perm;
  let dirname =
    if String.ends_with ~suffix:"/" path then
      path
    else
      path ^ "/" in
  Impl.{dirname; file_perm}

let open_store ?(file_perm=0o700) path =
  try
    if Sys.is_directory path then
      let dirname =
        if String.ends_with ~suffix:"/" path then
          path
        else
          path ^ "/" in
      Ok Impl.{dirname; file_perm}
    else
      Result.error @@
      `Store_read (path ^ " is not a Filesystem store.")
  with
  | Sys_error _ ->
    Result.error @@ `Store_read (path ^ " does not exist.")

let open_or_create ?(file_perm=0o700) path =
  match open_store ~file_perm path with
  | Ok v -> Ok v
  | Error _ -> Ok (create ~file_perm path)
