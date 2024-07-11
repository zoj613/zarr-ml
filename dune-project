(lang dune 3.15)

(name zarr)

(generate_opam_files true)

(source
 (github zoj613/zarr-ml))

(authors "Zolisa Bleki")

(maintainers "Zolisa Bleki")

(license BSD-3-Clause)

(documentation https://zoj613.github.io/zarr-ml/zarr/Zarr/index.html)

(package
 (name zarr)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends
   dune
   (ocaml (>= 4.14.2))
   yojson
   ezgzip
   owl
   stdint
   checkseum
   (odoc :with-doc)
   (ounit2 :with-test)
   (ppx_deriving :with-test)
   (bisect_ppx
     (and :dev (>= 2.5.0) :with-test)))
 (tags
  (topics "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
