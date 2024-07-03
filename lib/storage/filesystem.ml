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
    | Sys_error _ | End_of_file ->
      Error (`Store_read fpath)

  let set t key value =
    let filename = key_to_fspath t key in
    create_parent_dir filename t.file_perm;
    Out_channel.with_open_gen
      Out_channel.[Open_wronly; Open_trunc; Open_creat]
      t.file_perm
      filename
      (fun oc -> Out_channel.output_string oc value)

  let list t =
    let module StrSet = Storage_intf.Base.StrSet in
    let rec aux acc path =
      try
        match Sys.readdir path with
        | [||] -> acc
        | xs ->
          Array.fold_left (fun set x ->
            match path ^ x with
            | p when Sys.is_directory p ->
              aux set @@ p ^ "/"
            | p ->
              StrSet.add (fspath_to_key t p) set) acc xs 
      with
      | Sys_error _ -> acc
    in
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

  let get_partial_values t kr_pairs =
    Storage_intf.Base.get_partial_values
      ~get_fn:get t kr_pairs

  let set_partial_values t krv_triplet =
    Storage_intf.Base.set_partial_values
      ~set_fn:set ~get_fn:get t krv_triplet

  let erase_values t keys =
    Storage_intf.Base.erase_values
      ~erase_fn:erase t keys

  let erase_prefix t pre =
    Storage_intf.Base.erase_prefix
      ~list_fn:list ~erase_fn:erase t pre

  let list_prefix pre t =
    Storage_intf.Base.list_prefix ~list_fn:list t pre

  let list_dir t pre =
    Storage_intf.Base.list_dir ~list_fn:list t pre
end

let create ?(file_perm=0o640) path =
  Impl.create_parent_dir path file_perm;
  Sys.mkdir path file_perm;
  let dirname =
    if String.ends_with ~suffix:"/" path then
      path
    else
      path ^ "/" in
  Impl.{dirname; file_perm}

let open_store ?(file_perm=0o640) path =
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

let open_or_create ?(file_perm=0o640) path =
  try open_store ~file_perm path with
  | Sys_error _ -> Ok (create ~file_perm path)
