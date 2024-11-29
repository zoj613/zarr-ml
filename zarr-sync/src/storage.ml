module IO = struct
  type 'a t = 'a
  let return = Fun.id
  let bind x f = f x
  let map f x = f x
  let return_unit = ()
  let iter = List.iter
  let fold_left = List.fold_left
  let concat_map = List.concat_map

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
    type t = {dirname : string; perm : int}
    type 'a io = 'a IO.t

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos (String.length path - pos)

    let key_to_fspath t key = Filename.concat t.dirname key

    let get t key =
      try In_channel.(with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) input_all) with
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

    let size t key = 
      try In_channel.(with_open_gen [Open_rdonly] t.perm (key_to_fspath t key) length) |> Int64.to_int with
      | Sys_error _ -> 0

    let rec walk t acc dir =
      let accumulate ~t a x = match Filename.concat dir x with
        | p when Sys.is_directory p -> walk t a p
        | p -> (fspath_to_key t p) :: a
      in
      let dir_contents = Array.to_list (Sys.readdir dir) in
      List.fold_left (accumulate ~t) acc dir_contents
    
    let list_dir t prefix =
      let choose ~t ~dir x = match Filename.concat dir x with
        | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let dir = key_to_fspath t prefix in
      let dir_contents = Array.to_list (Sys.readdir dir) in
      List.partition_map (choose ~t ~dir) dir_contents

    let list t = walk t [] (key_to_fspath t "")
    let list_prefix t prefix = walk t [] (key_to_fspath t prefix)
    let erase t key = Sys.remove (key_to_fspath t key)
    let erase_prefix t pre = List.iter (erase t) (list_prefix t pre)
    let rename t k k' = Sys.rename (key_to_fspath t k) (key_to_fspath t k')
    let is_member t key = Sys.file_exists (key_to_fspath t key)
  end

  let create ?(perm=0o700) dirname =
    Zarr.Util.create_parent_dir dirname perm;
    Sys.mkdir dirname perm;
    S.{dirname = Zarr.Util.sanitize_dir dirname; perm}

  let open_store ?(perm=0o700) dirname =
    if Sys.is_directory dirname
    then S.{dirname = Zarr.Util.sanitize_dir dirname; perm}
    else raise (Zarr.Storage.Not_a_filesystem_store dirname)

  include Zarr.Storage.Make(IO)(S)
end

module HttpStore = struct
  exception Not_implemented
  exception Request_failed of int * string

  let raise_error (code, s) = raise (Request_failed (Curl.int_of_curlCode code, s))

  let fold_result = Result.fold ~error:raise_error ~ok:Fun.id

  module IO = struct
    module Deferred = Deferred

    type t =
      {tries : int
      ;base_url : string
      ;client : Ezcurl_core.t
      ;config : Ezcurl_core.Config.t}

    let get t key =
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let res = Ezcurl.get ~tries ~client ~config ~url () in
      match fold_result res with
      | {code; body; _} when code = 200 -> body
      | {code; body; _} -> raise (Request_failed (code, body))

    let size t key = try String.length (get t key) with
      | Request_failed (404, _) -> 0
    (*let size t key =  
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      print_endline @@ "about to HEAD " ^ url;
      let res = Ezcurl.http ~tries ~client ~config ~url ~meth:HEAD () in
      match fold_result res with
      | {code; _} when code = 404 ->
        print_endline "akho head";
        0
      | {headers; _} ->
        match List.assoc_opt "content-length" headers with
        | (Some "0" | None) ->
          begin try print_endline "empty content-length header"; String.length (get t key) with
          | Request_failed (404, _) -> 0 end
        | Some l -> int_of_string l *)

    let is_member t key = if (size t key) = 0 then false else true

    let get_partial_values t key ranges =
      let tries = t.tries and client = t.client and config = t.config and url = t.base_url ^ key in
      let fetch range = Ezcurl.get ~range ~tries ~client ~config ~url () in
      let end_index ofs l = Printf.sprintf "%d-%d" ofs (ofs + l - 1) in
      let read_range (ofs, len) =
        let none = Printf.sprintf "%d-" ofs in
        let range = Option.fold ~none ~some:(end_index ofs) len in
        let response = fold_result (fetch range) in
        response.body
      in
      List.map read_range ranges

    let set t key data =
      let tries = t.tries and client = t.client and config = t.config
      and url = t.base_url ^ key and content = `String data in
      let res = Ezcurl.post ~params:[] ~tries ~client ~config ~url ~content () in
      match fold_result res with
      | {code; _} when code = 200 || code = 201 -> ()
      | {code; body; _} -> raise (Request_failed (code, body))

    let set_partial_values t key ?(append=false) rsv =
      let size = size t key in
      let ov = match size with
        | 0 -> String.empty
        | _ -> get t key
      in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      set t key (List.fold_left f ov rsv)

    (* make reshaping arrays possible *)
    let erase t key =
      let tries = t.tries and client = t.client and config = t.config in
      let url = t.base_url ^ key in
      let res = Ezcurl.http ~tries ~client ~config ~url ~meth:DELETE () in
      match fold_result res with
      | {code; _} when code = 200 -> ()
      | {code; body; _} -> raise (Request_failed (code, body))

    let erase_prefix _ = raise Not_implemented
    let list _ = raise Not_implemented
    let list_dir _ = raise Not_implemented
    let rename _ = raise Not_implemented
  end

  let with_open ?(redirects=5) ?(tries=3) url f =
    let config = Ezcurl_core.Config.(default |> max_redirects redirects |> follow_location true) in
    let perform client = f IO.{tries; client; config; base_url = url ^ "/"} in
    Ezcurl_core.with_client perform

  include Zarr.Storage.Make(IO)
end
