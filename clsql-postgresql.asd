;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;; *************************************************************************
;;;; FILE IDENTIFICATION
;;;;
;;;; Name:          clsql-postgresql.asd
;;;; Purpose:       ASDF file for CLSQL PostgresSQL backend
;;;; Programmer:    Kevin M. Rosenberg
;;;; Date Started:  Aug 2002
;;;;
;;;; $Id: clsql-postgresql.asd,v 1.3 2002/09/01 09:00:15 kevin Exp $
;;;;
;;;; This file, part of CLSQL, is Copyright (c) 2002 by Kevin M. Rosenberg
;;;;
;;;; CLSQL users are granted the rights to distribute and use this software
;;;; as governed by the terms of the Lisp Lesser GNU Public License
;;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;; *************************************************************************

(declaim (optimize (debug 3) (speed 3) (safety 1) (compilation-speed 0)))
(in-package :asdf)

(defmethod source-file-type  ((c cl-source-file)
			      (s (eql (find-system 'clsql-postgresql)))) 
   "cl")

(defsystem clsql-postgresql
  :pathname #.(format nil "~A:clsql-postgresql;" +clsql-logical-host+)
  :components ((:file "postgresql-package")
	       (:file "postgresql-loader" :depends-on ("postgresql-package"))
	       (:file "postgresql-api" :depends-on ("postgresql-loader"))
	       (:file "postgresql-sql" :depends-on ("postgresql-api"))
	       (:file "postgresql-usql" :depends-on ("postgresql-sql")))
  :depends-on (:uffi :clsql-base :clsql-uffi))
