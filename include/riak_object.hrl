-define(DOT, <<"dot">>). %% The event at which a value was written, stored in metadata

%% Names of riak_object metadata fields
-define(MD_CTYPE,    <<"content-type">>).
-define(MD_CHARSET,  <<"charset">>).
-define(MD_ENCODING, <<"content-encoding">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LINKS,    <<"Links">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_INDEX,    <<"index">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).
-define(MD_VAL_ENCODING, <<"X-Riak-Val-Encoding">>).