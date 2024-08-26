type 'a t = 'a
let return x = x
let return_unit = ()
let iter = List.iter
let fold_left = List.fold_left
let concat_map = List.concat_map

module Infix = struct
  let (>>=) x f = f x
  let (>>|) = (>>=)
end
