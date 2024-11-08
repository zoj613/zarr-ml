module type S = sig
  include Storage.STORE

  val with_open :
    ?level:[ `None | `Fast | `Default | `Best ] ->
    ?perm:int ->
    [< `Read_only | `Read_write ] ->
    string ->
    (t -> 'a Deferred.t) ->
    'a Deferred.t
  (** [with_open mode p f] opens the zip archive at path [p] and applies
      function [f] to its open handle and writes any changes back to the zip
      archive if [mode] is [`Read_write], otherwise discards them at exit.
      If [p] does not exist, a handle to an empty zip archive is opened.
      Note that this function loads the entire zip archive bytes into memory,
      so care must be taken to ensure that these bytes can fit into the local
      machine's available memory. For now it does not handle ZIP64. ZIP64 is
      needed if your ZIP archive or decompressed file sizes exceed 2{^32}-1
      bytes or if you need more than 65535 archive members.

      {ul 
      {- [level] is the DEFLATE algorithm compression level used when writing
        data to the store and defaults to [`Default]. Choose [`None] for no
        compression, [`Fast] for best speed, [`Best] for high compression rate
        and [`Default] for a mix of good speed and compression rate.}
      {- [perm] is the file permission to use when opening an existing zip file
        and defaults to [0o700].}
      } *)
end

module Make (Deferred : Types.Deferred) : S with type 'a Deferred.t = 'a Deferred.t = struct
  open Deferred.Syntax

  let fold_kind ~dir ~file = function
    | Zipc.Member.Dir -> dir
    | Zipc.Member.File f -> file f

  let fold_result ~ok res = Result.fold ~error:failwith ~ok res

  module IO = struct
    module Deferred = Deferred

    type t = {ic : Zipc.t Atomic.t; level : Zipc_deflate.level}

    let is_member t key =
      let z = Atomic.get t.ic in
      Deferred.return (Zipc.mem key z)

    let size t key =
      let decompressed_size = function
        | None -> 0
        | Some m ->
          let entry_kind = Zipc.Member.kind m in
          fold_kind ~dir:0 ~file:Zipc.File.decompressed_size entry_kind
      in
      let z = Atomic.get t.ic in
      Deferred.return (decompressed_size @@ Zipc.find key z)

    let get t key =
      let to_string f = fold_result ~ok:Fun.id (Zipc.File.to_binary_string f) in
      let decompressed_value = function
        | None -> raise (Storage.Key_not_found key)
        | Some m ->
          let entry_kind = Zipc.Member.kind m in
          fold_kind ~dir:String.empty ~file:to_string entry_kind
      in
      let z = Atomic.get t.ic in
      Deferred.return (decompressed_value @@ Zipc.find key z)

    let get_partial_values t key ranges =
      let read_range ~data ~size (ofs, len) = match len with
        | None -> String.sub data ofs (size - ofs)
        | Some l -> String.sub data ofs l
      in
      let+ data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let list t =
      let accumulate_path m acc = Zipc.Member.path m :: acc in
      let z = Atomic.get t.ic in
      Deferred.return (Zipc.fold accumulate_path z [])

    let list_dir t prefix =
      let module S = Set.Make(String) in
      let accumulate ~prefix m ((l, r) as acc) =
        let key = Zipc.Member.path m in
        if not (String.starts_with ~prefix key) then acc else
        let n = String.length prefix in
        if not (String.contains_from key n '/') then key :: l, r else
        l, S.add String.(sub key 0 @@ 1 + index_from key n '/') r
      in
      let z = Atomic.get t.ic in 
      let ks, ps = Zipc.fold (accumulate ~prefix) z ([], S.empty) in
      Deferred.return (ks, S.elements ps)

    let rec set t key value =
      let res = Zipc.File.deflate_of_binary_string ~level:t.level value in
      let f = Zipc.Member.File (fold_result ~ok:Fun.id res) in
      let m = fold_result ~ok:Fun.id Zipc.Member.(make ~path:key f) in
      let z = Atomic.get t.ic in
      if Atomic.compare_and_set t.ic z (Zipc.add m z)
      then Deferred.return_unit else set t key value

    let rec set_partial_values t key ?(append=false) rv =
      let to_string f = fold_result ~ok:Fun.id (Zipc.File.to_binary_string f) in
      let empty =
        let res = Zipc.File.deflate_of_binary_string ~level:t.level String.empty in
        let res' = Zipc.Member.File (fold_result ~ok:Fun.id res) in
        fold_result ~ok:Fun.id Zipc.Member.(make ~path:key res')
      in
      let z = Atomic.get t.ic in
      let mem = Option.fold ~none:empty ~some:Fun.id (Zipc.find key z) in
      let ov = fold_kind ~dir:String.empty ~file:to_string (Zipc.Member.kind mem) in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      let ov' = List.fold_left f ov rv in
      let res = Zipc.File.deflate_of_binary_string ~level:t.level ov' in
      let file = Zipc.Member.File (fold_result ~ok:Fun.id res) in
      let m = fold_result ~ok:Fun.id Zipc.Member.(make ~path:key file) in
      if Atomic.compare_and_set t.ic z (Zipc.add m z)
      then Deferred.return_unit else set_partial_values t key ~append rv

    let rec erase t key =
      let z = Atomic.get t.ic in
      if Atomic.compare_and_set t.ic z (Zipc.remove key z)
      then Deferred.return_unit else erase t key

    let rec erase_prefix t prefix =
      let accumulate ~prefix m acc =
        if String.starts_with ~prefix (Zipc.Member.path m)
        then acc else Zipc.add m acc
      in
      let z = Atomic.get t.ic in
      let z' = Zipc.fold (accumulate ~prefix) z Zipc.empty in
      if Atomic.compare_and_set t.ic z z'
      then Deferred.return_unit else erase_prefix t prefix

    (* Adapted from: https://github.com/dbuenzli/zipc/issues/8#issuecomment-2392417890 *)
    let rec rename t prefix new_prefix =
      let accumulate ~prefix ~new_prefix m acc =
        let path = Zipc.Member.path m in
        if not (String.starts_with ~prefix path) then Zipc.add m acc else
        let l = String.length prefix in
        let path = new_prefix ^ String.sub path l (String.length path - l) in
        let mtime = Zipc.Member.mtime m in
        let mode = Zipc.Member.mode m in
        let kind = Zipc.Member.kind m in
        let m' = Zipc.Member.make ~mtime ~mode ~path kind in
        Zipc.add (fold_result ~ok:Fun.id m') acc
      in
      let z = Atomic.get t.ic in
      let z' = Zipc.fold (accumulate ~prefix ~new_prefix) z Zipc.empty in
      if Atomic.compare_and_set t.ic z z'
      then Deferred.return_unit else rename t prefix new_prefix
  end

  let with_open ?(level=`Default) ?(perm=0o700) mode path f =
    let write_to_disk ~perm ~path str =
      let write ~str oc = Out_channel.output_string oc str; flush oc in
      let flags = [Open_wronly; Open_trunc; Open_creat] in
      Out_channel.with_open_gen flags perm path (write ~str)
    in
    let make z = IO.{ic = Atomic.make z; level} in
    let x = if not (Sys.file_exists path) then make Zipc.empty else
      let s = In_channel.(with_open_bin path input_all) in
      fold_result ~ok:make (Zipc.of_binary_string s)
    in
    match mode with
    | `Read_only -> f x
    | `Read_write ->
      let+ out = f x in
      let str = Zipc.to_binary_string (Atomic.get x.ic) in
      fold_result ~ok:(write_to_disk ~perm ~path) str;
      out

  include Storage.Make(IO)
end
