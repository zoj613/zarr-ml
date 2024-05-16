open Interface

(* general implementation agnostic STORE interface functions *)

module StrSet = Set.Make (String)

let erase_values ~erase_fn t keys =
  StrSet.iter (erase_fn t) @@ StrSet.of_list keys

let erase_prefix ~list_fn ~erase_fn t pre =
  List.iter (fun k ->
    if String.starts_with ~prefix:pre k
    then begin
      erase_fn t k
    end) @@ list_fn t

let list_prefix ~list_fn t pre =
  List.filter
    (String.starts_with ~prefix:pre) 
    (list_fn t)

let list_dir ~list_fn t pre =
  let paths =
    List.map
      (fun k ->
        Result.get_ok @@
        Node.of_path @@
        String.cat "/" k)
      (list_prefix ~list_fn t pre)
  in
  let is_prefix_child k =
    match Node.parent k with
    | Some par ->
      String.equal pre @@ Node.to_prefix par
    | None -> false in
  let keys, rest =
    List.partition_map (fun k ->
      match is_prefix_child k with
      | true -> Either.left @@ Node.to_key k
      | false -> Either.right k)
    paths
  in
  let prefixes =
    List.fold_left (fun acc k ->
      match
        List.find_opt
          is_prefix_child
          (Node.ancestors k)
      with
      | None -> acc
      | Some v ->
        let w = Node.to_prefix v in
        if List.mem w acc then acc
        else w :: acc)
    [] rest
  in
  keys, prefixes

let rec get_partial_values ~get_fn t kr_pairs =
  match kr_pairs with
  | [] -> [None]
  | (k, r) :: xs ->
    match get_fn t k with
    | Error _ ->
      None :: (get_partial_values ~get_fn t xs)
    | Ok v ->
      try
        let sub = match r with
          | ByteRange (rs, None) ->
            String.sub v rs @@ String.length v 
          | ByteRange (rs, Some rl) ->
            String.sub v rs rl in
        Some sub :: (get_partial_values ~get_fn t xs)
      with
      | Invalid_argument _ ->
        None :: (get_partial_values ~get_fn t xs)
  
let rec set_partial_values ~set_fn ~get_fn t = function
  | [] -> Ok ()
  | (k, rs, v) :: xs ->
    match get_fn t k with
    | Error _ ->
      set_fn t k v;
      set_partial_values ~set_fn ~get_fn t xs
    | Ok ov ->
      try
        let ov' = Bytes.of_string ov in
        String.(length v |> blit v 0 ov' rs);
        set_fn t k @@ Bytes.to_string ov';
        set_partial_values ~set_fn ~get_fn t xs
      with
      | Invalid_argument s ->
        Error (`Invalid_byte_range s)
