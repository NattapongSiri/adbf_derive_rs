//! Assist user on creating disk based table/record.
//! The easiest way is
//! ```Rust
//! use adbf_derive_rs::{table};
//! #[table("path_to.dbf")]
//! struct SomeTable;
//! ```
//! This will load file "path_to.dbf" and populate
//! ```Rust
//! struct SomeTable {
//!     rows: Vec<SomeTableRec>,
//!     // All other meta fields
//! }
//! 
//! impl SomeTable {
//!     fn save(&self) {
//!         // save table to disk
//!     }
//! }
//! 
//! impl TableOps for SomeTable {
//!     type Row = SomeTableRec;
//! 
//!     fn insert_owned(&mut self, row: Self::Row) {
//!         self.rows.push(row);
//!     }
//! 
//!     fn len(&self) -> usize {
//!         self.rows.len()
//!     }
//! }
//! 
//! // Auto implement all necessary trait to satisfied trait TableOps requirements for SomeTable
//! 
//! struct SomeTableRec {
//!     /*
//!      * fields will be populated from the
//!      * dbf file.
//!      */
//! }
//! 
//! impl RecordOps for SomeTableRec {
//!     fn from_bytes(record: &[u8]) -> Self {
//!         /*
//!          * Auto implement parsing record bytes into `SomeTableRec`
//!          */
//!     }
//! 
//!     fn to_bytes(&self) -> Vec<u8> {
//!         /*
//!          * Auto implement record conversion into Vec<u8> so it
//!          * can be saved back to file
//!          */
//!     }
//! }
//! ```
#[cfg(feature="threaded")]
extern crate adbf_rs_threaded as adbf_rs; 

use adbf_rs::{
    DBFType,
    FieldMeta,
    Header,
    read_dbf_type,
    read_header,
    foxpro::{
        cp_mapper
    }
};
use futures::{
    executor::block_on,
    future::{
        BoxFuture,
        Future,
        Lazy,
        lazy
    },
    lock::Mutex,
    stream::Stream,
    task::{
        Context,
        Poll
    }
};
use proc_macro::{
    TokenStream
};
use proc_macro_roids::FieldsNamedAppend;
use quote::{
    format_ident,
    quote
};
use std::{
    fmt::{
        Debug,
        Display
    }, 
    fs::{File},
    path::Path
};
use syn::{
    bracketed,
    parse_macro_input, 
    parse_quote, 
    Ident,
    ItemStruct, 
    Lit,
    LitBool,
    LitInt,
    LitStr,
    MetaNameValue,
    Token,
    punctuated::Punctuated,
    parse::{
        Parse,
        ParseStream
    },
    Visibility
};

mod foxpro;

const DEFAULT_BUFFER_SIZE : usize = 1_048_576;

#[derive(Debug)]
enum TableType {
    Path(LitStr),
    Complex(TableOpDef)
}

impl Parse for TableType {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(syn::LitStr) {
            input.parse().map(TableType::Path)
        } else {
            println!("Parsing as complex");
            input.parse().map(TableType::Complex)
        }
    }
}

/// All attribute types used by table attribute macro
#[derive(Debug)]
enum TableAttributeType {
    Str(Ident, LitStr),
    Int(Ident, LitInt),
    Bool(Ident, LitBool),
    StrList(Ident, Punctuated<LitStr, Token![,]>)
}

impl TableAttributeType {
    fn ident(&self) -> Ident {
        use TableAttributeType::*;
        match self {
            Str(i, _) | Int(i, _) | Bool(i, _) | StrList(i, ..) => i.clone(),
        }
    }
}

impl Parse for TableAttributeType {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident: Ident = input.parse().expect("Expected identity");
        let _ : syn::token::Eq = input.parse().expect("Expected equal symbol");
        if input.peek(LitStr) {
            input.parse().map(|s| TableAttributeType::Str(ident, s))
        } else if input.peek(LitInt) {
            input.parse().map(|i| TableAttributeType::Int(ident, i))
        } else if input.peek(LitBool) {
            input.parse().map(|b| TableAttributeType::Bool(ident, b))
        } else if input.peek(syn::token::Bracket) {
            let list;
            bracketed![list in input];
            let str_list = Punctuated::<LitStr, Token![,]>::parse_terminated(&list).expect("Expecting string literal separated by comma");
            Ok(TableAttributeType::StrList(ident, str_list))
        } else {
            panic!("Unsupported attribute type")
        }
    }
}

#[derive(Debug)]
struct TableOpDef(Vec<TableAttributeType>);

/// Return name(s) of attribute that will yield compile error if it is missing.
fn verify_mandatory_table_att(atts: &Punctuated<TableAttributeType, Token![,]>) {
    let required_attributes = [
        "path"
    ];

    let missing: Vec<&str> = required_attributes.iter().filter_map(|r| {
        for t in atts.iter() {
            if t.ident() == r {
                return None;
            }
        }

        return Some(*r);
    }).collect();

    if missing.len() > 0 {
        let err = "Missing mandatory attribute(s)";
        panic!("{}\n{:?}", err, missing);

    }
}

/// Return a supported attributes to be used with table attribute macro.
/// It currently support
/// - `path` - A path to dbf file
/// - `auto_fields` - Auto generate a fields from given dbf file.
/// - `type` - A type of dbf, such as "vfp", "foxpro"
/// - `load_strategy` - how this table load the dbf file.
/// - `parse_strategy` - how each readed byte conversion is perform.
/// - `buffer` - Size of buffer to be used. This is useful for iteration use case. It won't be help if the usecase is random access.
/// - `include` - The name of columns to be included. Each name shall be separated by comma.
/// - `exclude` - The name of columns to be excluded. Each name shall be separated by comma.
#[inline(always)]
fn supported_table_att() -> &'static [&'static str] {
    &[
        // a path to dbf file.
        // If auto_fields is yes, the file in this path must already exist.
        "path",
        // should fields in record be derived from given dbf file
        "auto_fields",
        // a table type such as "vfp".
        // If type is missing, it'll attempt to read the dbf type from
        // dbf file in given path
        "type",
        // table loading strategy.
        // either load an entire table into memory.
        // or load each record on the fly and discard it content after parsed.
        "load_strategy",
        // record parsing strategy.
        // either return record that owned the value or
        // simply a ref to chunk of byte in memory hold in table.
        // the ref method need the table loading strategy to be in-memory.
        "parse_strategy",
        // buffer is an integer value of bytes in memory used to read the file.
        // this is useful when iterate over table
        "buffer",
        // the columns to be included as part of struct.
        // this imply auto_fields is true.
        // this option is mutual exclusive with "exclude".
        // if both options are provided, it will ignore
        // exclude option.
        "include",
        // the columns to be excluded from being part of struct.
        // this imply auto_fields is true.
        // this option is mutual exclusive with "include".
        // if both options are provided, it will ignore
        // exclude option.
        "exclude"
    ]
}

/// Utilities function that check if given attribute contains unknown attribute.
/// If there's at least one unknown attribute, it'll result in compile error.
fn verify_unknown_table_att(atts: &Punctuated<TableAttributeType, Token![,]>) {
    let supported_attributes = supported_table_att();

    let unknown: Vec<String> = atts.iter().filter_map(|a| {
        let ident = a.ident().to_string();
        if !supported_attributes.contains(&ident.as_str()) {
            Some(ident)
        } else {
            None
        }
    }).collect();

    if unknown.len() > 0 {
        let err = "Following attribute(s) isn't support.".to_string();
        panic!("{}\n{:?}", err, unknown);
    }
}

/// Parse table op attribute and stored it inside `Vec<MetaNameValue>`
impl Parse for TableOpDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let map: Punctuated<TableAttributeType, Token![,]> = input.parse_terminated(TableAttributeType::parse).unwrap();
        verify_mandatory_table_att(&map);
        verify_unknown_table_att(&map);

        Ok(TableOpDef(map.into_iter().map(|prop| {
            prop
        }).collect()))
    }
}

/// Supported table loading strategy
#[derive(Debug)]
enum TableLoadStrategy {
    /// Load entire table into memory when app loaded
    Eager,
    /// Load each record when needed such as in iteration or indexing operation
    Lazy
}

/// Support two mode of parsing strategy.
/// 1. Conversion - Each record is just a transmuted from raw bytes. There's very little
/// cost on each record parsing. However, there may be some cost when accessing the field.
/// 2. Parse - Each record is parsed from raw bytes into Rust datatype. There's a cost of
/// mapping/decoding/copying from raw bytes into Rust datatype but there's no further cost
/// on accessing each field.
/// Depending on usecase, one will be faster than other.
/// 
/// For example: 
/// - If return record will be compare for equality then end up being accessed 
/// for few times then `Conversion` will be a lot faster. This is because bytes base
/// equality is exactly the same as parsed bytes comparison. If the bytes are equals,
/// the parsed must be equals.
/// - If return record needed to be processed by semantic, such as `String` comparison
/// like field.to_lowercase() == "abc" || field == "123", the parsed will be a lot faster.
/// This is because accessing to the field result in decoding string. It need to decode
/// twice. 
/// One may try caching the field of `Conversion` type record to reduce number of parsing.
/// This is user choice to decided whether it's more efficient.
#[derive(Debug)]
enum RecordParsingStrategy {
    /// Use unsafe method to directly convert each byte chunks into Rust datatype.
    /// It use `std::mem::transmute` to perform datatype conversion then use
    /// `std::mem::forget` to force table to release an onwership of these bytes chunk.
    /// To prevent memory leak, all remaining chunks that is not owned by record will
    /// be freed by `std::mem::drop` such as case where user specify the table
    /// to return only particular columns by either specify `include` or `exclude` 
    /// attribute. There's some datatype that have cost on accessing to the field.
    /// This is because the transmuted bytes chunk cannot be directly interpret such as
    /// - `char` - it needed to be encoded/decoded.
    /// - `currency` - it needed to be divided by 10,000.
    /// - `Date` - it needed to be parsed into `NaiveDate`
    /// - `DateTime` - it needed to be parsed into `NaiveDateTime`
    /// If most of return record is of these types, using conversion will be slower
    Conversion,
    /// Use safe method by copy bytes chunk into record as Rust datatype.
    /// It perform type conversion immediately when parsed so there's no cost
    /// on accessing any field in the return record.
    Parse
}

/// The table metadata
#[derive(Debug)]
struct TableOption {
    path: String,
    auto_fields: bool,
    dbf_type: DBFType,
    load_strategy: TableLoadStrategy,
    parse_strategy: RecordParsingStrategy,
    buffer: usize,
    include: Vec<String>,
    exclude: Vec<String>
}

/// Convert from TableOpDef into TableOption where each attribute is well defined.
impl std::convert::From<TableOpDef> for TableOption {
    fn from(source: TableOpDef) -> Self {
        use TableAttributeType::*;
        let meta = source.0;
        let path = match meta.iter().find(|m| m.ident() == supported_table_att()[0]) {
            Some(p) => {
                match p {
                    Str(_, ref p) => p.value(),
                    _ => panic!("Expect path value to be literal string")
                }
            },
            _ => panic!("Missing mandatory attribute path")
        };
        let auto_fields = match meta.iter().find(|m| m.ident() == supported_table_att()[1]) {
            Some(f) => {
                match f {
                    Bool(_, ref b) => b.value,
                    _ => true
                }
            },
            None => true
        };
        let dbf_type = match meta.iter().find(|m| m.ident() == supported_table_att()[2]) {
            Some(t) => {
                match t {
                    Str(_, ref s) => {
                        match s.value().to_lowercase().as_str() {
                            "foxpro" | "vfp" | "visualfoxpro" => DBFType::VisualFoxPro,
                            _ => DBFType::Undefined
                        }
                    },
                    _ => {
                        panic!("The dbf type attribute must be literal string")
                    }
                }
            },
            None => {
                read_dbf_type(&path).expect("The given path is not valid dbf file format")
            }
        };
        let load_strategy = match meta.iter().find(|m| m.ident() == supported_table_att()[3]) {
            Some(t) => {
                match t {
                    Str(_, ref s) => {
                        match s.value().to_lowercase().as_str() {
                            "eager" => TableLoadStrategy::Eager,
                            "lazy" => TableLoadStrategy::Lazy,
                            _ => panic!("Only support eager or lazy load_strategy")
                        }
                    },
                    _ => {
                        panic!("The load_strategy attribute must be literal string")
                    }
                }
            },
            None => {
                TableLoadStrategy::Lazy
            }
        };
        let parse_strategy = match meta.iter().find(|m| m.ident() == supported_table_att()[4]) {
            Some(t) => {
                match t {
                    Str(_, ref s) => {
                        match s.value().to_lowercase().as_str() {
                            "conversion" => RecordParsingStrategy::Conversion,
                            "parse" => RecordParsingStrategy::Parse,
                            _ => panic!("Only support conversion or parse parse_strategy")
                        }
                    },
                    _ => {
                        panic!("The parse_strategy attribute must be literal string")
                    }
                }
            },
            None => {
                RecordParsingStrategy::Parse
            }
        };
        let buffer = match meta.iter().find(|m| m.ident() == supported_table_att()[5]) {
            Some(t) => {
                match t {
                    Int(_, ref b) => {
                        b.base10_parse::<usize>().expect("The buffer attribute must be positive integer")
                    },
                    _ => {
                        panic!("The buffer attribute must be positive integer")
                    }
                }
            },
            None => {
                DEFAULT_BUFFER_SIZE // 1MB
            }
        };
        let include = match meta.iter().find(|m| m.ident() == supported_table_att()[6]) {
            Some(t) => {
                match t {
                    Str(_, ref s) => {
                        s.value().split(",").map(|column| column.trim().to_string()).collect()
                    },
                    StrList(_, ref s) => {
                        s.iter().map(|column| column.value().trim().to_string()).collect()
                    },
                    _ => {
                        panic!("The include attribute must be literal string contains field name separated by comma")
                    }
                }
            },
            None => {
                Vec::with_capacity(0)
            }
        };
        let exclude = match meta.iter().find(|m| m.ident() == supported_table_att()[7]) {
            Some(t) => {
                match t {
                    Str(_, ref s) => {
                        s.value().split(",").map(|column| column.trim().to_string()).collect()
                    },
                    StrList(_, ref s) => {
                        s.iter().map(|column| column.value().trim().to_string()).collect()
                    },
                    _ => {
                        panic!("The exclude attribute must be literal string contains field name separated by comma")
                    }
                }
            },
            None => {
                Vec::with_capacity(0)
            }
        };
        TableOption {
            path: path,
            auto_fields: auto_fields,
            dbf_type: dbf_type,
            load_strategy: load_strategy,
            parse_strategy: parse_strategy,
            buffer: buffer,
            include: include,
            exclude: exclude
        }
    }
}

impl std::convert::From<TableType> for TableOption {
    fn from(source: TableType) -> Self {
        match source {
            TableType::Path(p) => {
                let path = p.value();
                let dbf_type = read_dbf_type(&path).expect("The given path is not valid dbf file format");
                
                TableOption {
                    path: path,
                    auto_fields: true,
                    dbf_type: dbf_type,
                    load_strategy: TableLoadStrategy::Lazy,
                    parse_strategy: RecordParsingStrategy::Parse,
                    buffer: DEFAULT_BUFFER_SIZE,
                    include: Vec::with_capacity(0),
                    exclude: Vec::with_capacity(0)
                }
            },
            TableType::Complex(c) => {
                TableOption::from(c)
            }
        }
    }
}

fn get_dbf_type<P: AsRef<Path> + Debug + Display>(path: P) -> DBFType {
    read_dbf_type(&path).expect("Fail to read dbf file")
}

fn gen_rec_ident(table_ident: &Ident) -> Ident {
    let mut id = table_ident.to_string();
    id.push_str("Rec");
    format_ident!("{}", id)
}

fn generate_foxpro_rec_struct(vis: &Visibility, id: &Ident, parse_strategy: &RecordParsingStrategy, fields: &mut [foxpro::Field], meta: &Header) -> proc_macro2::TokenStream {
    sort_fields_by_offset(fields);
    let fields_def = build_foxpro_fields(&fields, parse_strategy);
    let rec_def = quote! {
        #vis struct #id {
            #(#fields_def),*
        }
    };

    let impl_recops = implement_foxpro_recordops(&id, parse_strategy, &meta, fields);
    let impl_clone = impl_rec_clone(&id, fields);

    quote! {
        #rec_def
        #impl_clone
        #impl_recops
    }
}

fn build_exclusion_list(include: &[String], fields: &[impl FieldMeta]) -> Vec<String> {
    fields.iter().filter_map(|f| 
        if include.iter().find(|i| f.name() == i.as_str()).is_some() {
            None
        } else {
            Some(f.name().to_owned())
        }
    ).collect()
}

/// Remove field(s) from `fields` slice that are in both `fields` and `excluded` slice. 
/// Return true if one or more excluded field is mandatory (disallow null).
fn remove_unused_fields(fields: &mut Vec<impl FieldMeta>, excluded: &[String]) -> bool {
    let mut excluded_mandatory = false;
    excluded.iter().for_each(|e| {
        for i in 0..fields.len() {
            if fields[i].name() == e {
                fields.remove(i);
                if !fields[i].nullable() {
                    excluded_mandatory = true
                }
                break;
            }
        }
    });
    excluded_mandatory
}

fn sort_fields_by_offset(fields: &mut[impl FieldMeta]) {
    fields.sort_by(|i, j| {
        i.rec_offset().cmp(&j.rec_offset())
    });
}

/// Generate field definition and wrap the type with `Option` if it's nullable.
/// Otherwise, it will define type as is.
macro_rules! field_quote {
    ($nullable: expr, #$field: ident : $dec: ty) => {
        let wrapped = if $nullable {
            quote! {
                Option<#$dec>
            }
        } else {
            quote! {$dec}
        };

        quote! {
            #$field: #wrapped
        }
    };
}

fn build_foxpro_fields(fields: &[impl FieldMeta], strategy: &RecordParsingStrategy) -> Vec<proc_macro2::TokenStream> {    
    match strategy {
        RecordParsingStrategy::Conversion => {
            fields.into_iter().map(|f| {
                let name = format_ident!("{}", f.name());
                match f.datatype_flag() {
                    b'B' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawDoubleField
                        }
                    },
                    b'C' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawCharField
                        }
                    },
                    b'D' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawDateField
                        }
                    },
                    b'T' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawDateTimeField
                        }
                    }
                    b'N' | b'F' => {
                        field_quote! {
                            f.nullable(),
                            #name: adbf_rs::foxpro::RawFloatField
                        }
                    },
                    b'G' => {
                        field_quote! {
                            f.nullable(),
                            #name: adbf_rs::foxpro::RawGeneralField
                        }    
                    },
                    b'I' => {
                        field_quote! {
                            f.nullable(),
                            #name: adbf_rs::foxpro::RawIntegerField
                        }    
                    },
                    b'L' => {
                        field_quote! {
                            f.nullable(),
                            #name: adbf_rs::foxpro::RawBool
                        }    
                    },
                    b'V' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawVarCharField
                        }
                    },
                    b'Y' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawCurrencyField
                        }
                    },
                    b'Q' => {
                        field_quote! {
                            f.nullable(),
                            #name : adbf_rs::foxpro::RawVarBin
                        }
                    },
                    _ => {
                        panic!("Not yet implemented for {:#x} data type", f.datatype_flag())
                    }
                }
            }).collect()
        },
        RecordParsingStrategy::Parse => {
            fields.into_iter().map(|f| {
                let name = format_ident!("{}", f.name());

                match f.datatype_flag() {
                    b'B' | b'Y' => {
                        field_quote! {
                            f.nullable(),
                            #name : f64
                        }
                    },
                    b'C' | b'V' => {
                        field_quote! {
                            f.nullable(),
                            #name : String
                        }
                    },
                    b'D' => {
                        field_quote! {
                            f.nullable(),
                            #name : chrono::NaiveDate
                        }
                    }
                    b'N' | b'F' => {
                        field_quote! {
                            f.nullable(),
                            #name: f32
                        }
                    },
                    b'T' => {
                        field_quote! {
                            f.nullable(),
                            #name: chrono::NaiveDateTime
                        }    
                    },
                    b'G' => {
                        field_quote! {
                            f.nullable(),
                            #name: u32
                        }    
                    },
                    b'I' => {
                        field_quote! {
                            f.nullable(),
                            #name: i32
                        }    
                    },
                    b'L' => {
                        field_quote! {
                            f.nullable(),
                            #name: bool
                        }    
                    },
                    b'Q' => {
                        field_quote! {
                            f.nullable(),
                            #name: Vec<u8>
                        }    
                    },
                    _ => {
                        panic!("Not yet implemented for {:#x} data type", f.datatype_flag())
                    }
                }
            }).collect()
        }
    }
}

fn implement_foxpro_recordops(name: &Ident, strategy: &RecordParsingStrategy, meta: &Header, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    
    let from_bytes = match strategy {
        RecordParsingStrategy::Conversion => implement_foxpro_record_conversion(name, meta.codepage, fields),
        RecordParsingStrategy::Parse => implement_foxpro_record_parse(name, meta.codepage, fields)
    };

    let to_bytes = match strategy {
        RecordParsingStrategy::Conversion => implement_foxpro_to_bytes_conversion(meta.record_len, fields),
        RecordParsingStrategy::Parse => implement_foxpro_to_bytes_parse(meta.record_len, meta.codepage, fields)
    };
    
    quote! {
        impl RecordOps for #name {
            #from_bytes

            #to_bytes
            // fn to_bytes(&self) -> Vec<u8> {
            //     todo!("Return an owned vec of byte")
            // }
        }
    }
}

/// Implement clone for all the types of record
fn impl_rec_clone(rec_iden: &Ident, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    let clone_fields = fields.iter().map(|f| {
        let field_name = format_ident!("{}", f.name());
        quote! {
            #field_name: self.#field_name.clone()
        }
    });
    quote! {
        impl Clone for #rec_iden {
            fn clone(&self) -> #rec_iden {
                #rec_iden {
                    #(#clone_fields),*
                }
            }
        }
    }
}

/// Utility macro to generate fields by given nullable parameter,
/// field name, and proc_macro2::TokenStream of field instantiation.
/// If nullable is `true`, it'll wrap the field instantiation with 
/// `Option::Some()`.
/// The field instantiation statement must be wrap in quote! macro.
/// This macro doesn't evaluate any instantiate statement.
macro_rules! field_assign_quote {
    ($nullable: expr, #$field: ident : $dec: expr) => {
        let wrapped = if $nullable {
            quote! {
                Option::Some(#$dec)
            }
        } else {
            $dec
        };
        quote! {
            #$field: #wrapped
        }
    };
}

fn implement_foxpro_record_conversion(rec_struct_ident: &Ident, codepage: &str, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    let conversions = fields.iter().map(|f| {
        let bytes = quote! {
            &record[f.rec_offset()..(f.rec_offset() + f.size())]
        };
        // boolean need single byte. byte is cheap to copy so simply copy it.
        let byte = quote! {
            record[f.rec_offset()]
        };
        let field_name = format_ident!("{}", f.name());
        let int = f.size(); // for numeric, it's integer part. For varchar/varbin, it's length of field.
        let prec = f.precision();
        let datatype = f.datatype_flag();
        let instantiate_stmt = match datatype {
            b'B' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawDoubleField {
                            bytes: #bytes
                        }
                    }
                }
            },
            b'C' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawCharField {
                            bytes: #bytes,
                            encoding: #codepage
                        }
                    }
                }
            },
            b'D' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawDateField {
                            bytes: #bytes
                        }
                    }
                }
            },
            b'T' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawDateTimeField {
                            bytes: #bytes
                        }
                    }
                }
            }
            b'N' | b'F' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawFloatField {
                            bytes: #bytes,
                            integer: #int,
                            precision: #prec
                        }
                    }
                }
            },
            b'G' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawGeneralField {
                            bytes: #bytes
                        }
                    }
                }    
            },
            b'I' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawIntegerField {
                            bytes: #bytes
                        }
                    }
                }    
            },
            b'L' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawBool {
                            byte: #byte
                        }
                    }
                }    
            },
            b'V' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawVarCharField {
                            bytes: #bytes,
                            encoding: #codepage,
                            max_length: #int
                        }
                    }
                }
            },
            b'Y' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawCurrencyField {
                            bytes: #bytes
                        }
                    }
                }
            },
            b'Q' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {
                        adbf_rs::foxpro::RawVarBin {
                            bytes: #bytes,
                            max_length: #int
                        }
                    }
                }
            },
            _ => {
                panic!("Not yet implemented for {:#x} data type", datatype)
            }
        };
        instantiate_stmt
    });

    quote! {
        fn from_bytes(record: &[u8]) -> Self {
            #rec_struct_ident {
                #(#conversions),*
            }
        }
    }
}

fn implement_foxpro_record_parse(rec_struct_ident: &Ident, encoding: &str, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    let conversions = fields.iter().map(|f| {
        let offset = f.rec_offset();
        let int = f.size(); // for numeric, it's integer part. For varchar/varbin, it's length of field.
        let bytes = quote! {
            record[#offset..(#offset + #int)]
        };
        // boolean need single byte. byte is cheap to copy so simply copy it.
        let byte = quote! {
            record[#offset]
        };
        let field_name = format_ident!("{}", f.name());
        let prec = f.precision();
        let datatype = f.datatype_flag();
        let instantiate_stmt = match datatype {
            b'B' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote!{f64::from_le_bytes(#bytes.try_into().unwrap())}
                }
            },
            b'C' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! ({
                        let mut value = String::with_capacity(#int);
                        let (reason, readed, _) = adbf_rs::get_decoder(#encoding).decode_to_string(&#bytes, &mut value, true);
                        if readed != #int {
                            match reason {
                                encoding_rs::CoderResult::InputEmpty => {
                                    panic!("Insufficient record data. Expect {} but found {}", #int, readed)
                                },
                                encoding_rs::CoderResult::OutputFull => {
                                    panic!("Insufficient buffer to store converted string")
                                }
                            };
                        }
                        value
                    })
                }
            },
            b'D' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {chrono::NaiveDate::from_num_days_from_ce(i32::from_le_bytes(#bytes[0..4].try_into().unwrap()))}
                }
            },
            b'T' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! ({
                        let mut date_part = chrono::NaiveDate::from_num_days_from_ce(i32::from_le_bytes(#bytes[0..4].try_into().unwrap()));
                        let time_part = u32::from_le_bytes(self.bytes[4..8].try_into().unwrap());
                        date_part.and_hms((time_part / 3_600_000) % 24, (time_part / 60_000) % 60, (time_part / 1000) % 60)
                        date_part
                    })
                }
            }
            b'N' | b'F' => {
                field_assign_quote! {
                    f.nullable(), 
                    #field_name: quote! ({
                        let mut buffer = String::with_capacity(#bytes.len());
                        let (result, readed, _) = adbf_rs::get_decoder("ISO-8859-1").decode_to_string(&#bytes, &mut buffer, true);
                        if readed != #int + #prec {
                            match result {
                                encoding_rs::CoderResult::InputEmpty => panic!("Insufficient input to read for Numeric/Float field. Please report an issue."),
                                encoding_rs::CoderResult::OutputFull => panic!("Insufficient buffer to write for Numeric/Float field. Please report an issue.")
                            }
                        }
                        buffer.parse().expect("Fail to convert buffered string to float. Please file an issue.")
                    })
                }
            },
            b'G' => {
                field_assign_quote! {
                    f.nullable(), 
                    #field_name: quote! {u32::from_bytes(#bytes.try_into().unwrap())}
                }    
            },
            b'I' => {
                field_assign_quote! {
                    f.nullable(), 
                    #field_name: quote! {i32:from_le_bytes(#bytes.try_into().unwrap())}
                }    
            },
            b'L' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! {#byte > 0}
                }    
            },
            b'V' => {
                field_assign_quote! {
                    f.nullable(),
                    #field_name: quote! ({
                        let value = String::with_capacity(#int);
                        let (reason, readed, _) = adbf_rs::get_decoder(#encoding).decode_to_string(&#bytes, &mut value, true);
                        if readed != #int {
                            match reason {
                                encoding_rs::CoderResult::InputEmpty => {
                                    panic!("Insufficient record data. Expect {} but found {}", #int, readed)
                                },
                                encoding_rs::CoderResult::OutputFull => {
                                    panic!("Insufficient buffer to store converted string")
                                }
                            }
                        }
                        value
                    })
                }
            },
            b'Y' => {
                field_assign_quote! {
                    f.nullable(), 
                    #field_name: quote! {(i64::from_le_bytes(#bytes.try_into().expect("Fail to convert to currency")) as f64) / 10_000f64}
                }
            },
            b'Q' => {
                field_assign_quote! {
                    f.nullable(), 
                    #field_name: quote! {#bytes.to_owned()}
                }
            },
            _ => {
                panic!("Not yet implemented for {:#x} data type", datatype)
            }
        };
        instantiate_stmt
    });

    quote! {
        fn from_bytes(record: &[u8]) -> Self {
            #rec_struct_ident {
                #(#conversions),*
            }
        }
    }
}

fn implement_foxpro_to_bytes_conversion(len: usize, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    let byte_copies = fields.iter().map(|f| {
        let name = f.name();
        let offset = f.rec_offset();
        let size = f.size();
        let end = offset + size;

        if f.nullable() {
            quote! {
                if let Some(bytes) = self.#name.bytes {
                    result[#offset..#end].iter_mut().zip(bytes.iter()).for_each(|(r, b)| *r = *b);
                }
            }
        } else {
            quote! {
                result[#offset..#end].iter_mut().zip(self.#name.bytes.iter()).for_each(|(r, b)| *r = *b);
            }
        }
    });
    quote! {
        // #to_bytes
        fn to_bytes(&self) -> Vec<u8> {
            let mut result = vec![0;#len];
            #(#byte_copies)*
            result
        }
    }
}

fn implement_foxpro_to_bytes_parse(len: usize, encoding: &str, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    let mut prev_end = 0;
    let converted = fields.iter().map(|f| {
        let name = format_ident!("{}",f.name());
        let name_str = f.name();
        let offset = f.rec_offset();
        let size = f.size();
        let precision = f.precision();
        let datatype = f.datatype_flag();

        let maybe_pad_quote = if offset > prev_end {
            quote! {
                bytes.append(&mut vec![0; #offset - #prev_end]);
            } 
        } else {
            quote! {}
        };

        prev_end = offset + size;

        if f.nullable() {
            quote! {
                #maybe_pad_quote
                if let Some(v) = self.#name {
                    bytes.append(v.to_le_bytes());
                } else {
                    bytes.append([0;#size]);
                }
            }
        } else {
            let encoded = match datatype {
                b'B' | b'G' | b'I' => { // Double, General, Integer have to_le_bytes function
                    quote! {
                        bytes.extend(&self.#name.to_le_bytes());
                    }
                },
                b'C' | b'V' => {
                    quote! {
                        {
                            let mut buffer = vec![0; #size];
                            let (result, _, _, _) = adbf_rs::get_encoder(#encoding).encode_from_utf8(&self.#name, &mut buffer, true);
                            match result {
                                encoding_rs::CoderResult::InputEmpty => bytes.append(&mut buffer),
                                encoding_rs::CoderResult::OutputFull => panic!("Input too large for field {}", #name_str)
                            }
                        }
                    }
                },
                b'D' => {
                    quote! {
                        let mut buffer = [0;4];
                        adbf_core_rs::foxpro::date_to_bytes(&self.#name, &mut buffer);
                        bytes.extend(&buffer);
                    }
                },
                b'T' => {
                    quote! {
                        let mut buffer = [0;8];
                        adbf_core_rs::foxpro::date_time_to_bytes(&self.#name, &mut buffer);
                        bytes.extend(&buffer);
                    }
                },
                b'N' | b'F' => {
                    quote! {
                        {
                            let mut buffer = vec![0; #size];
                            let float_str = format!("{0:0>0decimal$.precision$}", self.#name, decimal=#size - #precision, precision=#precision);
                            let (result, _, writen,_) = adbf_rs::get_encoder("ISO-8859-1").encode_from_utf8(&float_str, &mut buffer, true);
                            if writen != #size {
                                match result {
                                    encoding_rs::CoderResult::InputEmpty => panic!("Insufficient input to read for Numeric/Float field. Please report an issue."),
                                    encoding_rs::CoderResult::OutputFull => panic!("Insufficient buffer to write for Numeric/Float field. Please report an issue.")
                                }
                            }
                            bytes.append(&mut buffer);
                        }
                    }
                },
                b'L' => {
                    quote! {
                        if self.#name {
                            bytes.push(1u8);
                        } else {
                            bytes.push(0);
                        }
                    }
                },
                b'Y' => {
                    quote! {
                        bytes.extend(&((self.#name * 10_000) as i64).to_le_bytes());
                    }
                },
                b'Q' => {
                    quote! {
                        bytes.extend(&self.#name);
                    }
                },
                _ => {
                    panic!("Not yet implemented for {:#x} data type", datatype)
                }
            };

            quote! {
                #maybe_pad_quote
                #encoded
            }
        }
    });

    quote! {
        // #to_bytes
        fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(#len);
            #(#converted)*
            bytes
        }
    }
}

macro_rules! interior_mut_ident {
    () => {
        quote! { std::cell::RefCell }
    };
}

#[cfg(feature="threaded")]
macro_rules! interior_mut_ident {
    () => {
        quote! { std::sync::RwLock }
    };
}

fn define_table(vis: &Visibility, table_iden: &Ident, rec_iden: &Ident, load_strategy: &TableLoadStrategy) -> proc_macro2::TokenStream {
    let int_mut = interior_mut_ident!();
    let lazy_recs_type = quote! {Vec<Option<#int_mut<#rec_iden>>>};
    match load_strategy {
        TableLoadStrategy::Eager => {
            quote! {
                #vis struct #table_iden {
                    recs: Vec<#rec_iden>
                }
            }
        },
        TableLoadStrategy::Lazy => {
            quote! {
                #vis struct #table_iden {
                    file: String,
                    recs: #lazy_recs_type
                }
            }
        }
    }
}

fn impl_table(table_iden: &Ident, load_strategy: &TableLoadStrategy, first_rec_offset: usize, rec_size: usize, dbf_file: &str) -> proc_macro2::TokenStream {
    // let interior_mut = interior_mut_ident!();
    let instantiate_stmt = match load_strategy {
        TableLoadStrategy::Eager => quote! {
            use std::io::{Read, Seek};

            #table_iden {
                recs: {
                    let file = std::io::File::open(#dbf_file).unwrap();
                    let mut buff = Vec::new();
                    if let Ok(loc) = file.seek(std::io::SeekFrom::Start(#first_rec_offset)) {
                        file.read_to_end(&mut buff);
                        return buff.as_slice().windows(#rec_size).step_by(#rec_size).collect();
                    } else {
                        panic!("File {} is corrupted", #dbf_file)
                    }
                }
            }
        },
        TableLoadStrategy::Lazy => quote! {
            #table_iden {
                file: #dbf_file.to_owned(),
                recs: Vec::new()
            }
        }
    };

    quote! {
        impl #table_iden {
            fn new() -> #table_iden {
                #instantiate_stmt
            }
        }
    }
}

// It cannot be done using index trait. The Index trait take &self as parameter but lazily load require to mut self.
// Thus it will require interior mutability on field to be access.
// Those interior mutability type will return arbitary type such as Ref for RefCell or RwLockReadGuard for RwLock.
// These two types lifetime will be ended inside the index function while the return reference needed to outlive
// this function.
// To make this work, create a new struct that implement Future trait then return the struct instead.
// However, this make caller responsible for buffering the readed value as each index will return
// new struct that also implement the same Future. Otherwise, each time the .await is call on future, it'll
// load content from file.
// fn impl_index_ops(table_ident: &Ident, rec_ident: &Ident, load_strategy: &TableLoadStrategy, first_rec_offset: usize, rec_size: usize) -> proc_macro2::TokenStream {
//     // let read_recs = quote! { (&*self.recs.borrow()) };
//     // #[cfg(feature="threaded")]
//     // let read_recs = quote! { (&*self.recs.read().unwrap()) };
//     // let mut_recs = quote! { (&mut *self.recs.borrow_mut()) };
//     // #[cfg(feature="threaded")]
//     // let mut_recs = quote! { (&mut *self.recs.write().unwrap()) };
//     // let lock_type = quote! { std::cell::Ref };
//     let int_mut_ident = interior_mut_ident!();

//     let access = match load_strategy {
//         TableLoadStrategy::Eager => {
//             quote! {
//                 #rec_ident::from_bytes(self.bytes[index])
//             }
//         },
//         TableLoadStrategy::Lazy => {
//             quote! {
//                 fn read_at(f: &str, o: u64) -> #rec_ident {
//                     use std::io::{Seek, SeekFrom, Read};
//                     let mut fp = std::fs::File::open(f).unwrap();
//                     let pos = fp.seek(SeekFrom::Start(o)).unwrap();
//                     let mut bytes = Vec::with_capacity(#rec_size);
//                     unsafe {
//                         bytes.set_len(#rec_size);
//                     }
//                     fp.read_exact(&mut bytes).unwrap();
//                     #rec_ident::from_bytes(bytes.as_slice())
//                 }
//                 let n = self.recs.len();
//                 if index < n {
//                     if let Some(ref r) = self.recs[index] {
//                         return r;
//                     }

//                     self.recs[index] = Some(#int_mut_ident::new(read_at(&self.file, (#first_rec_offset + index * #rec_size) as u64)));
//                 } else {
//                     for _ in (n..index) {
//                         self.recs[index] = None
//                     }
    
//                     self.recs[index] = Some(#int_mut_ident::new(read_at(&self.file, (#first_rec_offset + index * #rec_size) as u64)));
//                 }
                
//                 if let Some(ref r) = self.recs[index] {
//                     r
//                 } else {
//                     panic!("Fail to assign new record into existing buffer")
//                 }
//             } 
//         }
//     };

//     quote! {

//         impl std::ops::Index<usize> for #table_ident {
//             type Output=#int_mut_ident<#rec_ident>;

//             fn index(&self, index: usize) -> &#int_mut_ident<#rec_ident> {
//                 #access
//             }
//         }
//     }
// }

fn impl_table_ops(table_ident: &Ident, rec_ident: &Ident) -> proc_macro2::TokenStream {
    quote! {

    }
}

#[proc_macro_attribute]
pub fn table(attr: TokenStream, item: TokenStream) -> TokenStream {
    let type_meta = parse_macro_input!(attr as TableType);
    println!("raw_meta={:?}", type_meta);
    let table_options = TableOption::from(type_meta);
    println!("parsed_meta={:?}", table_options);
    let ast : ItemStruct = syn::parse(item).expect("Expected `table` attribute macro to be used with struct.");
    match ast.fields {
        syn::Fields::Unit => (),
        _ => {
            panic!("Only support unit struct")
        }
    }
    let dbf_type = get_dbf_type(&table_options.path);
    let rec;
    let impl_tab;
    // let impl_tab_idx;
    let tab;
    match dbf_type {
        DBFType::VisualFoxPro => {
            let table_meta = block_on(read_header(&table_options.path, foxpro::cp_mapper)).expect("Fail to read table meta data");
            impl_tab = impl_table(&ast.ident, &table_options.load_strategy, table_meta.first_record_position, table_meta.record_len, &table_options.path);
            
            let mut f = File::open(&table_options.path).expect("Fail to find the dbf file");
            let mut fields = block_on(foxpro::read_fields(&mut f, &table_meta));
            let mut insertable = if table_options.include.len() > 0 {
                let exclusion = build_exclusion_list(&table_options.include, &fields);
                remove_unused_fields(&mut fields, &exclusion)
            } else {
                remove_unused_fields(&mut fields, &table_options.exclude)
            };
            let rec_id = gen_rec_ident(&ast.ident);
            tab = define_table(&ast.vis, &ast.ident, &rec_id, &table_options.load_strategy);
            rec = generate_foxpro_rec_struct(&ast.vis, &rec_id, &table_options.parse_strategy, fields.as_mut_slice(), &table_meta);
            // impl_tab_idx = impl_index_ops(&ast.ident, &rec_id, &table_options.load_strategy, table_meta.first_record_position, table_meta.record_len);
        },
        _ => {
            unimplemented!("The {:?} is not yet support", dbf_type);
        }
    }
    TokenStream::from(quote! {
        #tab
        #impl_tab
        // #impl_tab_idx
        #rec
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
