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
extern crate proc_macro;

use adbf_rs::{
    DBFType,
    FieldMeta,
    Header,
    read_dbf_type,
    read_header,
    foxpro::{
        cp_mapper,
        Field
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
    }
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

fn generate_foxpro_rec_struct(table: &ItemStruct, options: &TableOption, meta: &Header) -> proc_macro2::TokenStream {
    let mut id = table.ident.to_string();
    id.push_str("Rec");
    let mut f = File::open(&options.path).expect("Fail to find the dbf file");
    let mut fields = block_on(foxpro::read_fields(&mut f, meta));
    if options.include.len() > 0 {
        let exclusion = build_exclusion_list(&options.include, &fields);
        remove_unused_fields(&mut fields, &exclusion);
    } else {
        remove_unused_fields(&mut fields, &options.exclude);
    }
    sort_fields_by_offset(&mut fields);
    let fields_def = build_foxpro_fields(&fields, &options.parse_strategy);
    let id = format_ident!("{}", id);
    let vis = &table.vis;
    let rec_def = quote! {
        #vis struct #id {
            #(#fields_def),*
        }
    };
    let impl_recops = implement_foxpro_recordops(&id, &options.parse_strategy, fields.as_slice());

    quote! {
        #rec_def
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

fn remove_unused_fields(fields: &mut Vec<impl FieldMeta>, excluded: &[String]) {
    excluded.iter().for_each(|e| {
        for i in 0..fields.len() {
            if fields[i].name() == e {
                fields.remove(i);
                break;
            }
        }
    });
}

fn sort_fields_by_offset(fields: &mut[impl FieldMeta]) {
    fields.sort_by(|i, j| {
        i.rec_offset().cmp(&j.rec_offset())
    });
}

fn build_foxpro_fields(fields: &[impl FieldMeta], strategy: &RecordParsingStrategy) -> Vec<proc_macro2::TokenStream> {    
    match strategy {
        RecordParsingStrategy::Conversion => {
            fields.into_iter().map(|f| {
                let name = f.name();
                match f.datatype_flag() {
                    b'B' => {
                        quote! {
                            #name : f64
                        }
                    },
                    b'C' => {
                        quote! {
                            #name : adbf_rs::foxpro::RawCharField
                        }
                    },
                    b'D' => {
                        quote! {
                            #name : adbf_rs::foxpro::RawDateField
                        }
                    }
                    b'N' | b'F' => {
                        quote! {
                            #name: adbf_rs::foxpro::RawFloatField
                        }
                    }
                    _ => {
                        panic!("Not yet implemented for {:#x} data type", f.datatype_flag())
                    }
                }
            }).collect()
        },
        RecordParsingStrategy::Parse => {
            fields.into_iter().map(|f| {
                let name = f.name();

                match f.datatype_flag() {
                    b'B' => {
                        quote! {
                            #name : f64
                        }
                    },
                    b'C' => {
                        quote! {
                            #name: String
                        }
                    },
                    b'D' => {
                        quote! {
                            #name : chrono::NaiveDate
                        }
                    }
                    b'N' | b'F' => {
                        quote! {
                            #name: f32
                        }
                    }
                    _ => {
                        panic!("Not yet implemented for {:#x} data type", f.datatype_flag())
                    }
                }
            }).collect()
        }
    }
}

fn implement_foxpro_recordops(name: &Ident, strategy: &RecordParsingStrategy, fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    
    let from_bytes = match strategy {
        RecordParsingStrategy::Conversion => implement_foxpro_record_conversion(fields),
        RecordParsingStrategy::Parse => implement_foxpro_record_parse(fields)
    };
    quote! {
        impl RecordOps for #name {
            fn from_bytes(record: &[u8]) -> Self {
                todo!("Implement conversion/parsing based on requirement")
            }
            fn to_bytes(&self) -> Vec<u8> {
                todo!("Return an owned vec of byte")
            }
        }
    }
}

fn implement_foxpro_record_conversion(fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    todo!()
}

fn implement_foxpro_record_parse(fields: &[impl FieldMeta]) -> proc_macro2::TokenStream {
    todo!()
}

#[proc_macro_attribute]
pub fn table(attr: TokenStream, item: TokenStream) -> TokenStream {
    let type_meta = parse_macro_input!(attr as TableType);
    println!("raw_meta={:?}", type_meta);
    let table_options = TableOption::from(type_meta);
    println!("parsed_meta={:?}", table_options);
    let ast : ItemStruct = syn::parse(item).expect("Expected to `table` attribute macro to be used with struct.");
    match ast.fields {
        syn::Fields::Unit => (),
        _ => {
            panic!("Only support unit struct")
        }
    }
    let dbf_type = get_dbf_type(&table_options.path);
    println!("{:?}", ast);
    let rec;
    match dbf_type {
        DBFType::VisualFoxPro => {
            let table_meta = block_on(read_header(&table_options.path, foxpro::cp_mapper)).expect("Fail to read table meta data");
            rec = generate_foxpro_rec_struct(&ast, &table_options, &table_meta);
        },
        _ => {
            unimplemented!("The {:?} is not yet support", dbf_type);
        }
    }
    println!("{:?}", quote! {
        #ast
        #rec
    });
    TokenStream::from(quote! {
        #ast
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
