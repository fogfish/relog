%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%

%% reference
-define(_XSD_ANYURI,    0).

%% string
-define(_XSD_STRING,    1).

%% integer
-define(_XSD_INTEGER,   2).

-define(_XSD_BYTE,      3).
-define(_XSD_SHORT,     4).
-define(_XSD_INT,       5).
-define(_XSD_LONG,      6).

%% float
-define(_XSD_DECIMAL,   7).
-define(_XSD_FLOAT,     8).
-define(_XSD_DOUBLE,    9).

%% boolean
-define(_XSD_BOOLEAN,   10).

%% date
-define(_XSD_DATETIME,  11).
-define(_XSD_DATE,      12).
-define(_XSD_TIME,      13).
-define(_XSD_YEARMONTH, 14).
-define(_XSD_MONTHDAY,  15).
-define(_XSD_YEAR,      16).
-define(_XSD_MONTH,     17).
-define(_XSD_DAY,       18).

%% geo hash
-define(_GEORSS_POINT,  19).
-define(_GEORSS_HASH,   20).
-define(_GEORSS_JSON,   21).
