module ExtPoint = struct
  type 'a t =
    {name : string
    ;configuration : 'a}

  let ( = ) cmp x y =
    (x.name = y.name) &&
    cmp x.configuration y.configuration
end

module ArrayMap = struct
  include Map.Make (struct
    type t = int array
    let compare = Stdlib.compare
  end)

  let add_to_list k v map =
    update k (function
      | None -> Some [v]
      | Some l -> Some (v :: l)) map
end

module Result_syntax = struct
  let ( >>= ) = Result.bind

  let ( >>| ) x f =  (* infix map *)
    match x with
    | Ok v -> Ok (f v)
    | Error _ as e -> e
end

module Indexing = struct
  let rec cartesian_prod = function
    | [] -> [[]]
    | x :: xs ->
      List.concat_map (fun i ->
        List.map (List.cons i) (cartesian_prod xs)) x

  let range ?(step=1) start stop =
    List.of_seq @@ if step > 0 then
      Seq.unfold (function
        | x when x > stop -> None
        | x -> Some (x, x + step)) start
    else
      let start, stop = stop, start in
      Seq.unfold (function
        | x when x < start -> None
        | x -> Some (x, x + step)) stop

  (* get indices from a reformated slice *)
  let indices_of_slice = function
    | Owl_types.R_ [|start; stop; step|] -> range ~step start stop
    | Owl_types.L_ l -> Array.to_list l
    (* this is added for exhaustiveness but is never reached since
      a reformatted slice replaces a I_ index with an R_ index.*)
    | _ -> failwith "Invalid slice index."

  let reformat_slice slice shape =
    match slice with
    | [||] -> [||]
    | xs ->
      Owl_slicing.check_slice_definition
        (Owl_slicing.sdarray_to_sdarray xs) shape

  let coords_of_slice slice shape =
    (Array.map indices_of_slice @@
      reformat_slice slice shape)
    |> Array.to_list
    |> cartesian_prod
    |> List.map Array.of_list
    |> Array.of_list

  let slice_of_coords = function
    | [] -> [||]
    | xs ->
      let ndims = Array.length @@ List.hd xs in
      let indices = Array.make ndims [] in
      Array.map (fun x -> Owl_types.L x) @@
      List.fold_right (fun x acc ->
        Array.iteri (fun i y ->
          if List.mem y acc.(i) then ()
          else acc.(i) <- y :: acc.(i)) x; acc) xs indices

  let slice_shape slice array_shape =
    Owl_slicing.calc_slice_shape @@
      reformat_slice slice array_shape
end

let get_name j =
  Yojson.Safe.Util.(member "name" j |> to_string)

let prod x =
  Array.fold_left Int.mul 1 x

let max x =
  Array.fold_left
    (fun acc v ->
      if v <= acc then acc else v) Int.min_int x
