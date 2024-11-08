module type S = sig
  include Storage.STORE
  val create : unit -> t
  (** [create ()] returns a new In-memory Zarr store type.*)
end

module Make (Deferred : Types.Deferred) : S with type 'a Deferred.t = 'a Deferred.t = struct
  open Deferred.Syntax

  module M = Map.Make(String)

  module IO = struct
    module Deferred = Deferred

    type t = string M.t Atomic.t

    let get : t -> string -> string Deferred.t = fun t key ->
      match M.find_opt key (Atomic.get t) with
      | None -> raise (Storage.Key_not_found key)
      | Some v -> Deferred.return v

    let rec set : t -> string -> string -> unit Deferred.t = fun t key value ->
      let m = Atomic.get t in
      if Atomic.compare_and_set t m (M.add key value m)
      then Deferred.return_unit else set t key value 

    let list : t -> string list Deferred.t = fun t ->
      let m = Atomic.get t in
      Deferred.return @@ M.fold (fun k _ acc -> k :: acc) m []

    let is_member : t -> string -> bool Deferred.t = fun t key ->
      let m = Atomic.get t in
      Deferred.return (M.mem key m)

    let rec erase : t -> string -> unit Deferred.t = fun t key ->
      let m = Atomic.get t in
      let m' = M.update key (Fun.const None) m in
      if Atomic.compare_and_set t m m'
      then Deferred.return_unit else erase t key

    let size : t -> string -> int Deferred.t = fun t key ->
      match M.find_opt key (Atomic.get t) with
      | None -> Deferred.return 0
      | Some e -> Deferred.return (String.length e)

    let rec erase_prefix : t -> string -> unit Deferred.t = fun t prefix ->
      let pred ~prefix k v = if String.starts_with ~prefix k then None else Some v in
      let m = Atomic.get t in
      let m' = M.filter_map (pred ~prefix) m in
      if Atomic.compare_and_set t m m'
      then Deferred.return_unit else erase_prefix t prefix

    let get_partial_values :
      t -> string -> (int * int option) list -> string list Deferred.t
      = fun t key ranges ->
      let read_range ~data ~size (ofs, len) = match len with
        | Some l -> String.sub data ofs l
        | None -> String.sub data ofs (size - ofs)
      in
      let+ data = get t key in
      let size = String.length data in
      List.map (read_range ~data ~size) ranges

    let rec set_partial_values :
      t -> string -> ?append:bool -> (int * string) list -> unit Deferred.t
      = fun t key ?(append=false) rv ->
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
      then Deferred.return_unit else set_partial_values t key ~append rv

    let list_dir : t -> string -> (string list * string list) Deferred.t = fun t prefix ->
      let module S = Set.Make(String) in
      let add ~size ~prefix key _ ((l, r) as acc) =
        if not (String.starts_with ~prefix key) then acc else
        if not (String.contains_from key size '/') then key :: l, r else
        l, S.add String.(sub key 0 @@ 1 + index_from key size '/') r
      in
      let size = String.length prefix in
      let m = Atomic.get t in
      let keys, prefixes = M.fold (add ~prefix ~size) m ([], S.empty) in
      Deferred.return (keys, S.elements prefixes)

    let rec rename :
      t -> string -> string -> unit Deferred.t
      = fun t prefix new_prefix ->
      let add ~prefix ~new_prefix k v acc =
        if not (String.starts_with ~prefix k) then M.add k v acc else
        let l = String.length prefix in
        let k' = new_prefix ^ String.sub k l (String.length k - l) in
        M.add k' v acc
      in
      let m = Atomic.get t in
      let m' = M.fold (add ~prefix ~new_prefix) m M.empty in
      if Atomic.compare_and_set t m m'
      then Deferred.return_unit else rename t prefix new_prefix
  end

  let create : unit -> IO.t = fun () -> Atomic.make M.empty

  include Storage.Make(IO)
end
