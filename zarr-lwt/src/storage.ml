module Deferred = struct
  type 'a t = 'a Lwt.t
  let return = Lwt.return
  let bind = Lwt.bind
  let map = Lwt.map
  let return_unit = Lwt.return_unit
  let iter = Lwt_list.iter_s
  let fold_left = Lwt_list.fold_left_s
  let concat_map f l = Lwt.map List.concat (Lwt_list.map_p f l)

  module Infix = struct
    let (>>=) = Lwt.Infix.(>>=)
    let (>>|) = Lwt.Infix.(>|=) 
  end

  module Syntax = struct
    let (let*) = Lwt.bind
    let (let+) x f = Lwt.map f x
  end
end

module ZipStore = Zarr.Zip.Make(Deferred)
module MemoryStore = Zarr.Memory.Make(Deferred)

module FilesystemStore = struct
  module IO = struct
    module Deferred = Deferred
    open Deferred.Infix
    open Deferred.Syntax

    type t = {dirname : string; perm : Lwt_unix.file_perm}

    let fspath_to_key t path =
      let pos = String.length t.dirname + 1 in
      String.sub path pos (String.length path - pos)

    let key_to_fspath t key = Filename.concat t.dirname key

    let rec create_parent_dir fn perm =
      let maybe_create ~perm parent_dir = function
        | true -> Lwt.return_unit
        | false ->
          let* () = create_parent_dir parent_dir perm in
          Lwt_unix.mkdir parent_dir perm
      in
      let parent_dir = Filename.dirname fn in
      Lwt_unix.file_exists parent_dir >>= maybe_create ~perm parent_dir

    let size t key =
      let file_length path () = Lwt.map Int64.to_int (Lwt_io.file_length path)
      and filepath = key_to_fspath t key in
      Lwt.catch (file_length filepath) (Fun.const @@ Deferred.return 0)

    let get t key =
      let* buf_size = size t key in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create buf_size)
        ~flags:[Unix.O_RDONLY]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        Lwt_io.read

    let get_partial_values t key ranges =
      let max_range ~tot acc (s, l) = match l with
        | None -> Int.max acc (tot - s) 
        | Some rs -> Int.max acc rs
      in
      let read_range ~tot ~ic (ofs, len) =
        let* () = Lwt_io.set_position ic (Int64.of_int ofs) in
        match len with
        | None -> Lwt_io.read ~count:(tot - ofs) ic
        | Some count -> Lwt_io.read ~count ic
      in
      let* tot = size t key in
      let buf_size = List.fold_left (max_range ~tot) 0 ranges in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create buf_size)
        ~flags:[Unix.O_RDONLY]
        ~perm:t.perm
        ~mode:Lwt_io.Input
        (key_to_fspath t key)
        (fun ic -> Lwt_list.map_s (read_range ~tot ~ic) ranges)

    let set t key value =
      let write ~value oc = Lwt_io.write oc value in
      let filename = key_to_fspath t key in
      let* () = create_parent_dir filename t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create (String.length value))
        ~flags:Unix.[O_WRONLY; O_TRUNC; O_CREAT]
        ~perm:t.perm
        ~mode:Lwt_io.Output
        filename
        (write ~value)

    let set_partial_values t key ?(append=false) rvs =
      let write_all rvs oc =
        let write ~oc (ofs, value) =
          let* () = Lwt_io.set_position oc (Int64.of_int ofs) in
          Lwt_io.write oc value
        in
        Lwt_list.iter_s (write ~oc) rvs
      in
      let l = List.fold_left (fun a (_, s) -> Int.max a (String.length s)) 0 rvs in
      let flags = match append with
        | false -> Unix.[O_WRONLY; O_CREAT] 
        | true -> Unix.[O_APPEND; O_WRONLY; O_CREAT] 
      in
      let filepath = key_to_fspath t key in
      let* () = create_parent_dir filepath t.perm in
      Lwt_io.with_file
        ~buffer:(Lwt_bytes.create l)
        ~perm:t.perm
        ~mode:Lwt_io.Output
        ~flags
        filepath
        (write_all rvs)

    let rec walk t acc dir =
      let accumulate ~t x a =
        if x = "." || x  = ".." then Lwt.return a else
        match Filename.concat dir x with
        | p when Sys.is_directory p -> walk t a p
        | p -> Lwt.return (fspath_to_key t p :: a)
      in
      Lwt_stream.fold_s (accumulate ~t) (Lwt_unix.files_of_directory dir) acc

    let list_dir t prefix =
      let choose ~t ~dir x = match Filename.concat dir x with
        | p when Sys.is_directory p -> Either.right @@ (fspath_to_key t p) ^ "/"
        | p -> Either.left (fspath_to_key t p)
      in
      let predicate x = if x = "." || x = ".." then false else true in
      let dir = key_to_fspath t prefix in
      let relevant = Lwt_stream.filter predicate (Lwt_unix.files_of_directory dir) in
      let+ dir_contents = Lwt_stream.to_list relevant in
      List.partition_map (choose ~t ~dir) dir_contents

    let list t = walk t [] (key_to_fspath t "")
    let list_prefix t prefix = walk t [] (key_to_fspath t prefix)
    let is_member t key = Lwt_unix.file_exists (key_to_fspath t key)
    let erase t key = Lwt_unix.unlink (key_to_fspath t key)
    let erase_prefix t pre = list_prefix t pre >>= Lwt_list.iter_s (erase t)
    let rename t k k' = Lwt_unix.rename (key_to_fspath t k) (key_to_fspath t k')
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

module AmazonS3Store = struct
  module Credentials = Aws_s3_lwt.Credentials
  module S3 = Aws_s3_lwt.S3

  open Deferred.Infix
  open Deferred.Syntax

  exception Request_failed of S3.error

  let empty_content () = S3.{
    storage_class = Standard;
    meta_headers = None;
    etag = String.empty;
    key = String.empty;
    last_modified = 0.;
    size = 0
  }

  let fold_or_catch ~not_found res =
    let return_or_raise r () = match r with
      | Ok v -> Deferred.return v
      | Error e -> raise (Request_failed e)
    and on_exception ~not_found = function
      | Request_failed S3.Not_found -> Lwt.return (not_found ())
      | exn -> raise exn
    in
    Lwt.catch (return_or_raise res) (on_exception ~not_found)

  let raise_not_found k () = raise (Zarr.Storage.Key_not_found k)
  let empty_Ls = Fun.const ([], S3.Ls.Done)

  let fold_continuation ~return ~more = function
    | S3.Ls.Done -> Deferred.return return
    | S3.Ls.More continuation ->
      continuation () >>= fold_or_catch ~not_found:empty_Ls >>= fun (xs, cont) ->
      more xs cont

  module IO = struct
    module Deferred = Deferred

    type t =
      {retries : int
      ;bucket : string
      ;cred : Credentials.t
      ;endpoint : Aws_s3.Region.endpoint}

    let size t key =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.head ~bucket ~credentials ~key ~endpoint () in
      let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
      Lwt.map (fun (x : S3.content) -> x.size) (fold_or_catch ~not_found:empty_content res)

    let is_member t key = Lwt.map (fun s -> if s = 0 then false else true) (size t key)

    let get t key =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.get ~bucket ~credentials ~endpoint ~key () in
      let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
      fold_or_catch ~not_found:(raise_not_found key) res

    let get_partial_values t key ranges =
      let read_range t key (ofs, len) =
        let range = match len with
          | None -> S3.{first = Some ofs; last = None}
          | Some l -> S3.{first = Some ofs; last = Some (ofs + l - 1)}
        in
        let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
        let f ~endpoint () = S3.get ~bucket ~credentials ~endpoint ~range ~key () in
        let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
        Lwt.map (fun x -> [x]) (fold_or_catch ~not_found:(raise_not_found key) res)
      in
      Deferred.concat_map (read_range t key) ranges

    let set t key data =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.put ~bucket ~credentials ~endpoint ~data ~key () in
      let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
      let* _ = fold_or_catch ~not_found:(Fun.const String.empty) res in
      Deferred.return_unit

    let set_partial_values t key ?(append=false) rsv =
      let* size = size t key in
      let* ov = match size with
        | 0 -> Deferred.return String.empty
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

    let erase t key =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.delete ~bucket ~credentials ~endpoint ~key () in
      S3.retry ~retries:t.retries ~endpoint ~f () >>= fold_or_catch ~not_found:(Fun.const ())

    let rec delete_keys t cont () = 
      let del t xs c = Deferred.iter (delete_content t) xs >>= delete_keys t c in
      fold_continuation ~return:() ~more:(del t) cont

    and delete_content t S3.{key; _} = erase t key

    and erase_prefix t prefix =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.ls ~bucket ~credentials ~endpoint ~prefix () in
      let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
      let* xs, rest = fold_or_catch ~not_found:empty_Ls res in
      Deferred.iter (delete_content t) xs >>= delete_keys t rest

    let rec list t =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.ls ~bucket ~credentials ~endpoint () in
      let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
      let* xs, rest = fold_or_catch ~not_found:empty_Ls res in
      accumulate_keys (List.map content_key xs) rest

    and content_key S3.{key; _} = key

    and accumulate_keys acc cont =
      let append acc xs c = accumulate_keys (acc @ List.map content_key xs) c in
      fold_continuation ~return:acc ~more:(append acc) cont

    module S = Set.Make(String)

    let rec partition_keys prefix ((l, r) as acc) cont =
      let split ~acc ~prefix xs c = partition_keys prefix (List.fold_left (add prefix) acc xs) c in
      fold_continuation ~return:(l, S.elements r) ~more:(split ~acc ~prefix) cont

    and add prefix (l, r) (c : S3.content) =
      let size = String.length prefix in
      if not (String.contains_from c.key size '/') then c.key :: l, r else
      l, S.add String.(sub c.key 0 @@ 1 + index_from c.key size '/') r

    and list_dir t prefix =
      let bucket = t.bucket and credentials = t.cred and endpoint = t.endpoint in
      let f ~endpoint () = S3.ls ~bucket ~credentials ~endpoint ~prefix () in
      let* res = S3.retry ~retries:t.retries ~endpoint ~f () in
      let* xs, rest = fold_or_catch ~not_found:empty_Ls res in
      let init = List.fold_left (add prefix) ([], S.empty) xs in
      partition_keys prefix init rest

    let rec rename t prefix new_prefix =
      let upload t (k, v) = set t k v in
      let* xs = list t in
      let to_delete = List.filter (String.starts_with ~prefix) xs in
      let* data = Deferred.fold_left (rename_and_add ~t ~prefix ~new_prefix) [] to_delete in
      let* () = Deferred.iter (upload t) data in
      Deferred.iter (erase t) to_delete

    and rename_and_add ~t ~prefix ~new_prefix acc k =
      let l = String.length prefix in
      let k' = new_prefix ^ String.sub k l (String.length k - l) in
      Lwt.map (fun a -> (k', a) :: acc) (get t k)
  end

  let with_open ?(scheme=`Http) ?(inet=`V4) ?(retries=3) ~region ~bucket ~profile f =
    let* res = Credentials.Helper.get_credentials ~profile () in
    let cred = Result.fold ~ok:Fun.id ~error:raise res in
    let endpoint = Aws_s3.Region.endpoint ~inet ~scheme region in
    f IO.{bucket; cred; endpoint; retries}

  include Zarr.Storage.Make(IO)
end
