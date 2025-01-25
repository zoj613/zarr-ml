module type S = sig
  exception Path_already_exists of string
  include Storage.S
  val open_store : ?level:Codecs.deflate_level -> string -> t
  (** [open_store ?level p] returns a store instance representing a zip
      archive of a Zarr v3 hierarchy stored at path [p]. [level] is the DEFLATE
      algorithm compression setting used when writing new entries into the archive. *)

  val create : ?level:Codecs.deflate_level -> string -> t io
  (** [create ?level p] creates a zip archive at path [p] and then returns a
      store instance representing the zip archive. [level] is the DEFLATE algorithm
      compression setting used when writing new entries into the archive.

      @raise Path_already_exists if a file already exists at path [p]. *)
end

module Make (IO : Types.IO) : S with type 'a io := 'a IO.t = struct
  open IO.Syntax

  let with_open_in path f =
    let* ic = IO.map Zip.open_in (IO.return path) in
    Fun.protect ~finally:(fun () -> Zip.close_in ic) (fun () -> IO.return (f ic))

  let with_open_out path f =
    let* oc = IO.map Zip.open_update (IO.return path) in
    Fun.protect ~finally:(fun () -> Zip.close_out oc) (fun () -> IO.return (f oc))

  module Store = struct
    type t = {path : string; level : int}
    type 'a io = 'a IO.t

    let is_member t key =
      let entry_exists ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> false
        | _ -> true
      in 
      with_open_in t.path (entry_exists ~key)

    let size t key =
      let entry_size ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> 0
        | e -> e.uncompressed_size
      in
      with_open_in t.path (entry_size ~key)

    let get t key =
      let read_entry ~key ic = match Zip.find_entry ic key with
        | exception Not_found -> raise (Storage.Key_not_found key)
        | e -> Zip.read_entry ic e
      in
      with_open_in t.path (read_entry ~key)

    let get_partial_values t key ranges =
      let read_range ~data ~size (ofs, len) = match len with
        | Some l -> String.sub data ofs l
        | None -> String.sub data ofs (size - ofs)
      in
      let+ data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let list t =
      let to_filename : Zip.entry -> string = fun e -> e.filename in
      let get_keys ic = List.map to_filename (Zip.entries ic) in
      with_open_in t.path get_keys

    module StrSet = Set.Make(String)

    let list_dir t prefix =
      let n = String.length prefix in
      let add_entry_with_prefix ((l, r) as acc) = function
        | (e : Zip.entry) when not (String.starts_with ~prefix e.filename) -> acc
        | e when String.contains_from e.filename n '/' ->
          let key = e.filename in
          let pre = String.sub key 0 (1 + String.index_from key n '/') in
          StrSet.add pre l, r
        | e -> l, e.filename :: r
      in
      let+ entries = with_open_in t.path Zip.entries in
      let prefs, keys = List.fold_left add_entry_with_prefix (StrSet.empty, []) entries in
      keys, StrSet.elements prefs

    let set t key value =
      let level = t.level in
      with_open_out t.path (fun oc -> Zip.add_entry ~level value oc key)

    let set_partial_values t key ?(append=false) rvs =
      let* ov = try get t key with
        | Storage.Key_not_found _ -> IO.return String.empty
      in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      set t key (List.fold_left f ov rvs)

    let add_to_zip ~oc ~level (path, v) = Zip.add_entry ~level v oc path

    let rename t prefix new_prefix =
      let add_pair ~ic ~prefix ~new_prefix acc = function
        | (e : Zip.entry) when not (String.starts_with ~prefix e.filename) ->
          (e.filename, Zip.read_entry ic e) :: acc
        | e ->
          let l = String.length prefix in
          let path = new_prefix ^ String.sub e.filename l (String.length e.filename - l) in
          (path, Zip.read_entry ic e) :: acc
      in
      let rename_entries ic = List.fold_left (add_pair ~ic ~prefix ~new_prefix) [] (Zip.entries ic) in
      let* pairs = with_open_in t.path rename_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      let level = t.level in
      with_open_out t.path (fun oc -> List.iter (add_to_zip ~oc ~level) pairs)

    let erase t key =
      let filter ~ic acc = function
        | (e : Zip.entry) when e.filename = key -> acc
        | e -> (e.filename, Zip.read_entry ic e) :: acc
      in
      let filter_entries ic = List.fold_left (filter ~ic) [] (Zip.entries ic) in
      let* pairs = with_open_in t.path filter_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path (fun oc -> List.iter (add_to_zip ~oc ~level:t.level) pairs)

    let erase_prefix t prefix =
      let filter ~ic ~prefix acc = function
        | (e : Zip.entry) when String.starts_with ~prefix e.filename -> acc
        | e -> (e.filename, Zip.read_entry ic e) :: acc
      in
      let filter_entries ic = List.fold_left (filter ~ic ~prefix) [] (Zip.entries ic) in
      let* pairs = with_open_in t.path filter_entries in
      let oc = Zip.open_out t.path in Zip.close_out oc;  (* truncate the old zip file *)
      with_open_out t.path (fun oc -> List.iter (add_to_zip ~oc ~level:t.level) pairs)
  end

  exception Path_already_exists of string

  let open_store ?(level=Codecs.L6) path =
    let l = match level with
      | L0 -> 0 | L1 -> 1 | L2 -> 2 | L3 -> 3 | L4 -> 4
      | L5 -> 5 | L6 -> 6 | L7 -> 7 | L8 -> 8 | L9 -> 9
    in
    Store.{path; level = l}

  let create ?(level=Codecs.L6) path =
    if Sys.file_exists path then raise (Path_already_exists path)
    else
      let* oc = IO.map Zip.open_out (IO.return path) in
      Fun.protect ~finally:(fun () -> Zip.close_out oc) (fun () -> IO.return (open_store ~level path))

  include Storage.Make(IO)(Store)
end
