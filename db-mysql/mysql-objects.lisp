;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;; *************************************************************************
;;;; FILE IDENTIFICATION
;;;;
;;;; Name:     mysql-objects.lisp
;;;; Purpose:  CLSQL Object layer for MySQL
;;;; Created:  May 2004
;;;;
;;;; CLSQL users are granted the rights to distribute and use this software
;;;; as governed by the terms of the Lisp Lesser GNU Public License
;;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;; *************************************************************************

(in-package #:clsql-mysql)

(defmethod database-get-type-specifier ((type symbol) args database
                                        (db-type (eql :mysql)))
  (declare (ignore args database db-type))
  (case type
    (wall-time "DATETIME")
    (tinyint "TINYINT")
    (smallint "SMALLINT")
    (mediumint "MEDIUMINT")
    (t (call-next-method))))

