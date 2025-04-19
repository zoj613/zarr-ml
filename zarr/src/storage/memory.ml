type error = [ `Read of string ]

module type S = sig
  include Storage.S with type error = error
  val create : unit -> t
  (** [create ()] returns a new In-memory Zarr store type.*)
end

module Make (IO : Types.IO) : S with type 'a io := 'a IO.t = struct
  open IO.Syntax
  module M = Map.Make(String)

  module Store = struct
    type 'a io = 'a IO.t
    type t = string M.t Atomic.t
    type nonrec error = error
 
    let get t key =
      let x = M.find_opt key (Atomic.get t) in
      IO.lift (Option.to_result ~none:(`Zarr (`Read (Printf.sprintf "key %s not found" key))) x)

    let rec set t key value =
      let m = Atomic.get t in
      if Atomic.compare_and_set t m (M.add key value m)
      then IO.return_unit else set t key value 

    let list t =
      let m = Atomic.get t in
      IO.return (M.fold (fun k _ acc -> k :: acc) m [])

    let is_member t key =
      let m = Atomic.get t in
      IO.return (M.mem key m)

    let rec erase t key =
      let m = Atomic.get t in
      let m' = M.update key (Fun.const None) m in
      if Atomic.compare_and_set t m m'
      then IO.return_unit else erase t key

    let size t key =
      let x = M.find_opt key (Atomic.get t) in
      IO.return (Option.fold ~none:0 ~some:String.length x)

    let rec erase_prefix t prefix =
      let pred ~prefix k v = if String.starts_with ~prefix k then None else Some v in
      let m = Atomic.get t in
      let m' = M.filter_map (pred ~prefix) m in
      if Atomic.compare_and_set t m m'
      then IO.return_unit else erase_prefix t prefix

    let get_partial_values t key (ranges : Types.range list) =
      let read_range ~data ~size (ofs, len) =
        Option.fold ~none:String.(sub data ofs (size - ofs)) ~some:String.(sub data ofs) len
      in
      let+ data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let rec set_partial_values t key ?(append=false) (rv : (int * string) list) =
      let m = Atomic.get t in
      let ov = Option.fold ~none:String.empty ~some:Fun.id (M.find_opt key m) in
      let f = if append || ov = String.empty then
        fun acc (_, v) -> acc ^ v else
        fun acc (rs, v) ->
          let s = Bytes.unsafe_of_string acc in
          Bytes.blit_string v 0 s rs String.(length v);
          Bytes.unsafe_to_string s
      in
      let m' = M.add key (List.fold_left f ov rv) m in
      if Atomic.compare_and_set t m m'
      then IO.return_unit else set_partial_values t key ~append rv

    module StrSet = Set.Make(String)

    let list_dir t prefix =
      let add ~size ~prefix key _ ((l, r) as acc) =
        if not (String.starts_with ~prefix key) then acc else
        if not (String.contains_from key size '/') then key :: l, r else
        l, StrSet.add String.(sub key 0 @@ 1 + index_from key size '/') r
      in
      let size = String.length prefix in
      let m = Atomic.get t in
      let keys, prefixes = M.fold (add ~prefix ~size) m ([], StrSet.empty) in
      IO.return (keys, StrSet.elements prefixes)

    let rec rename t prefix new_prefix =
      let add ~prefix ~new_prefix k v acc =
        if not (String.starts_with ~prefix k) then M.add k v acc else
        let l = String.length prefix in
        let k' = new_prefix ^ String.sub k l (String.length k - l) in
        M.add k' v acc
      in
      let m = Atomic.get t in
      let m' = M.fold (add ~prefix ~new_prefix) m M.empty in
      if Atomic.compare_and_set t m m'
      then IO.return_unit else rename t prefix new_prefix
  end

  let create : unit -> Store.t = fun () -> Atomic.make M.empty
  include Storage.Make(IO)(Store)
end
