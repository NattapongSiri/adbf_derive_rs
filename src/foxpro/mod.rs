use adbf_rs::{FieldMeta, Header};
use core::fmt::Display;
use encoding_rs::{CoderResult, Decoder, Encoding};
use std::fmt;
use std::{
    convert::TryInto, 
    fs::File, 
    io::{
        Read, Seek, SeekFrom
    }, 
    task::{
        Waker
    }
};

use proc_macro2::{
    Span,
    TokenStream
};
use quote::TokenStreamExt;

use super::*;

#[cfg(test)]
mod tests;


/// Read field meta data from dbf file.
/// 
/// ## Field Subrecords Structure
/// ---
/// | Byte offset | Description |
/// ---
/// | 0 - 10 | Field name with a maximum of 10 chars. If less than 10, right hand padded with 0 |
/// | 11 | Field type:<br/>C - Character<br/>Y - Currency<br/>N - Numeric<br/>F - Float<br/>D - Date<br/> T - DateTime<br/>B - Double<br/>I - Integer<br/>L - Logical<br/>M - Memo<br/>G - General<br/>C - Character(Binary)<br/>M - Memo(binary)<br/>P - Picture<br/>+ - Autoincrement(dBase 7)<br/>O - Double(dbase 7)@ - Timestamp(dbase 7) |
/// | 12 - 15 | Displacement of field in record |
/// | 16 | Length of field (bytes) |
/// | 17 | Number of decimal places |
/// | 18 | Field flags:<br/>0x01 System Column (not visible to user)<br/>0x02 Column is nullable<br/>0x04 Binary column<br/>0x0C Column is autoincrement |
/// | 19 - 22 | Value of next autoincrement |
/// | 23 | Value of autoincrement step |
/// | 24 - 31 | Reserved |
pub async fn read_fields(f: &mut File, h: &Header) -> Vec<Field> {
    f.seek(SeekFrom::Start(32)).expect("Fail to move file cursor to fields meta data");
    let mut buffer = [0u8;32];
    let encoding = match Encoding::for_label(h.codepage.as_bytes()) {
        Some(e) => e,
        None => panic!("Encoding {} is not supported", &h.codepage)
    };

    let mut decoder = encoding.new_decoder();
    let mut fields = vec![];
    f.read_exact(&mut buffer).expect("Fail to read file");
    let field_count = (h.first_record_position - 32) / 32;
    let mut count = 1;

    while let Some(field) = read_field_meta(buffer.try_into().unwrap(), &mut decoder) {
        fields.push(field);
        
        if count >= field_count {
            break;
        }

        f.read_exact(&mut buffer).expect("Fail to read file");
        count = count + 1;
    }

    fields
}

fn read_field_meta(bytes: [u8; 32], decoder: &mut Decoder) -> Option<Field> {
    if bytes[0] == 0x0D {
        return None
    }
    
    let mut field_name = String::with_capacity(11);
    // print!("Total bytes readed is {:?}", bytes);
    let (reason, readed, _) = decoder.decode_to_string(&bytes[0..12], &mut field_name, true);
    if readed != 11 {
        match reason {
            CoderResult::InputEmpty => {
                panic!("Fail to read field name from meta data")
            },
            CoderResult::OutputFull => {
                panic!("Insufficient field name length allocated. Please file a defect report.")
            }
        }
    }
    // let flag = match std::str::from_utf8(&bytes[11..12]) {
    //     Ok(s) => s,
    //     Err(err) => {
    //         panic!(err)
    //     }
    // };
    // let field_type = FieldType::from_flag(&flag).unwrap();
    let field_type = bytes[11];
    let offset = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let size = bytes[16] as usize;
    let precision = bytes[17] as usize;
    let flag = bytes[18];

    // auto increment next id
    let next_id = u32::from_le_bytes(bytes[19..23].try_into().unwrap());
    // auto increment step
    let next_step = bytes[24] as u32;

    Some(Field {
        name: field_name,
        datatype_flag: field_type,
        offset: offset,
        size: size,
        precision: precision,
        next_id: next_id,
        step: next_step,
        system: match flag & 0x01 == 1 {
            true => Some(()),
            false => None
        },
        nullable: match flag & 0x02 == 1 {
            true => Some(()),
            false => None
        },
        binary: match flag & 0x04 == 4 {
            true => Some(()),
            false => None
        },
        autoincrement: match flag & 0x0C == 0x0C {
            true => Some(()),
            false => None
        }
    })
}

pub fn cp_mapper(codepage: u8) -> Result<&'static str, &'static str> {
    match codepage {
        1 => Ok("cp437"),
        2 => Ok("cp850"),
        3 => Ok("cp1252"),
        4 => Ok("cp10000"),
        100 => Ok("cp852"),
        101 => Ok("cp866"),
        102 => Ok("cp865"),
        103 => Ok("cp861"),
        104 => Ok("cp895"),
        105 => Ok("cp620"),
        106 => Ok("cp737"),
        107 => Ok("cp857"),
        120 => Ok("cp950"),
        121 => Ok("cp949"),
        122 => Ok("cp936"),
        123 => Ok("cp932"),
        124 => Ok("TIS-620"),
        125 => Ok("cp1255"),
        126 => Ok("cp1256"),
        150 => Ok("cp10007"),
        151 => Ok("cp10029"),
        152 => Ok("cp10006"),
        200 => Ok("cp1250"),
        201 => Ok("cp1251"),
        202 => Ok("cp1254"),
        203 => Ok("cp1253"),
        _ => Result::Err("Unknown codepage found")
    }
}

#[derive(Clone)]
pub enum FieldType {
    /// Fixed length character data type
    Character,
    /// 8 bytes integer divide by 10,000 so it can contains at most 4 digit precisions.
    Currency,
    /// 8 bytes Date. A day count since 1/1/0001
    Date,
    /// 8 bytes DateTime. 4 first bytes is date. 4 later bytes is time.
    DateTime,
    /// IEEE compatible floating point format
    Double,
    /// Float/Numeric - Store as ASCII text on disk but 8 bytes in memory
    Float,
    /// OLE Object
    General,
    /// 32 bit integer
    Integer,
    /// 1 bytes logical data representation as True/False
    Logical,
    /// 4 bytes represent an offset inside memo file
    Memo,
    /// Same as float. Store as ASCII text on disk but 8 bytes in memory.
    Numeric,
    Picture,
    /// Store variable length binary data
    Varbinary,
    /// Store variable length character
    Varchar
}

impl FieldType {
    pub fn from_flag(f: &str) -> Result<FieldType, &'static str> {
        match f.chars().nth(0).unwrap() {
            'C' => Ok(FieldType::Character),
            'Y' => Ok(FieldType::Currency),
            'D' => Ok(FieldType::Date),
            'T' => Ok(FieldType::DateTime),
            'B' => Ok(FieldType::Double),
            'F' => Ok(FieldType::Float),
            'G' => Ok(FieldType::General), 
            'I' => Ok(FieldType::Integer),
            'L' => Ok(FieldType::Logical),
            'M' => Ok(FieldType::Memo),
            'P' => Ok(FieldType::Picture),
            'Q' => Ok(FieldType::Varbinary),
            'V' => Ok(FieldType::Varchar),
            _ => Err("Unsupported flag")
        }
    }
}

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub datatype_flag: u8,
    pub offset: usize,
    pub size: usize,
    pub precision: usize,
    pub next_id: u32,
    pub step: u32,
    pub nullable: Option<()>,
    pub system: Option<()>,
    pub autoincrement: Option<()>,
    pub binary: Option<()>
}

impl FieldMeta for Field {
    fn nullable(&self) -> bool {
        self.nullable.is_some()
    }
    fn autoincrement(&self) -> bool {
        self.autoincrement.is_some()
    }
    fn datatype_flag(&self) -> u8 {
        self.datatype_flag
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }
    fn rec_offset(&self) -> usize {
        self.offset
    }
    fn size(&self) -> usize {
        self.size
    }
    fn precision(&self) -> usize {
        self.precision
    }
    fn next_id(&mut self) -> u32 {
        self.next_id
    }
}

#[derive(Clone)]
pub struct CharField<'a> {
    pub meta: Field,
    content: String,
    codepage: &'a str,
    ready: Option<()>,
    record: &'a [u8]
}

impl<'a> FieldMeta for CharField<'a> {
    fn nullable(&self) -> bool {
        self.meta.nullable()
    }
    fn autoincrement(&self) -> bool {
        self.meta.autoincrement()
    }
    fn datatype_flag(&self) -> u8 {
        b'C'
    }
    fn name(&self) -> &str {
        self.meta.name()
    }
    fn rec_offset(&self) -> usize {
        self.meta.rec_offset()
    }
    fn size(&self) -> usize {
        self.meta.size()
    }
    fn precision(&self) -> usize {
        self.meta.precision()
    }
    fn next_id(&mut self) -> u32 {
        self.meta.next_id()
    }
}

impl<'a> Display for CharField<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.content)
    }
}
// impl<'a> FieldOps for CharField<'a> {

//     fn from_record_bytes(&mut self) -> BoxFuture<()> {
//         Box::pin(async move {
//             let field = &self.record[self.meta.rec_offset()..(self.meta.rec_offset() + self.meta.size())];
//             let (reason, readed, _) = get_decoder(self.codepage).decode_to_string(field, &mut self.content, true);
//             if readed != self.meta.size() {
//                 match reason {
//                     CoderResult::InputEmpty => {
//                         panic!("Insufficient record data. Expect {} but found {}", self.meta.size(), readed)
//                     },
//                     CoderResult::OutputFull => {
//                         panic!("Insufficient buffer to store converted string")
//                     }
//                 }
//             }
//         })
//     }

//     fn to_bytes(&self) -> BoxFuture<&[u8]> {
//         Box::pin(
//             async move {
//                 &self.record[self.meta.rec_offset()..(self.meta.size() + self.meta.rec_offset())]
//             }
//         )
//     }

//     fn ready(&self) -> bool {
//         self.ready.is_some()
//     }
// }

// #[derive(Clone)]
// pub struct CurrencyField<'a> {
//     pub meta: Field,
//     content: String,
//     ready: Option<()>,
//     record: &'a [u8]
// }

// impl<'a> FieldMeta for CurrencyField<'a> {
//     fn nullable(&self) -> bool {
//         self.meta.nullable()
//     }
//     fn autoincrement(&self) -> bool {
//         self.meta.autoincrement()
//     }
//     fn name(&self) -> &str {
//         self.meta.name()
//     }
//     fn rec_offset(&self) -> usize {
//         self.meta.rec_offset()
//     }
//     fn size(&self) -> usize {
//         self.meta.size()
//     }
//     fn precision(&self) -> usize {
//         self.meta.precision()
//     }
//     fn next_id(&mut self) -> u32 {
//         self.meta.next_id()
//     }
// }

// impl<'a> Display for CurrencyField<'a> {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.content)
//     }
// }

// impl<'a> FieldOps for CurrencyField<'a> {

//     fn from_record_bytes(&mut self) -> BoxFuture<()> {
//         Box::pin(async move {
//             let field = &self.record[self.meta.rec_offset()..(self.meta.rec_offset() + self.meta.size())];
            
//             let raw = i64::from_le_bytes(field.try_into().unwrap());
//             let integer = raw / 10000;
//             let fraction = raw % 10000;
//             self.content = format!("{}.{:04}", integer, fraction);
//         })
//     }

//     fn to_bytes(&self) -> BoxFuture<&[u8]> {
//         Box::pin(
//             async move {
//                 &self.record[self.meta.rec_offset()..(self.meta.rec_offset() + self.meta.size())]
//             }
//         )
//     }

//     fn ready(&self) -> bool {
//         self.ready.is_some()
//     }
// }

// #[derive(Clone)]
// pub struct DateField<'a> {
//     pub meta: Field,
//     content: NaiveDate,
//     ready: Option<()>,
//     record: &'a [u8]
// }

// impl<'a> FieldMeta for DateField<'a> {
//     fn nullable(&self) -> bool {
//         self.meta.nullable()
//     }
//     fn autoincrement(&self) -> bool {
//         self.meta.autoincrement()
//     }
//     fn name(&self) -> &str {
//         self.meta.name()
//     }
//     fn rec_offset(&self) -> usize {
//         self.meta.rec_offset()
//     }
//     fn size(&self) -> usize {
//         self.meta.size()
//     }
//     fn precision(&self) -> usize {
//         self.meta.precision()
//     }
//     fn next_id(&mut self) -> u32 {
//         self.meta.next_id()
//     }
// }

// impl<'a> Display for DateField<'a> {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.content)
//     }
// }

// impl<'a> FieldOps for DateField<'a> {

//     fn from_record_bytes(&mut self) -> BoxFuture<()> {
//         Box::pin(
//             async move {
//                 let field = &self.record[self.meta.rec_offset()..(self.meta.rec_offset() + self.meta.size())];
//                 self.content = NaiveDate::from_num_days_from_ce(i64::from_le_bytes(field.try_into().unwrap()) as i32);
//             }
//         )
//     }

//     fn to_bytes(&self) -> BoxFuture<&[u8]> {
//         Box::pin(
//             async move {
//                 &self.record[self.meta.rec_offset()..(self.meta.rec_offset() + self.meta.size())]
//             }
//         )
//     }

//     fn ready(&self) -> bool {
//         self.ready.is_some()
//     }
// }

// #[derive(Clone)]
// pub struct DateTimeField<'a> {
//     pub meta: Field,
//     ready: Option<()>,
//     content: NaiveDateTime,
//     record: &'a [u8]
// }

// impl<'a> FieldMeta for DateTimeField<'a> {
//     fn nullable(&self) -> bool {
//         self.meta.nullable()
//     }
//     fn autoincrement(&self) -> bool {
//         self.meta.autoincrement()
//     }
//     fn name(&self) -> &str {
//         self.meta.name()
//     }
//     fn rec_offset(&self) -> usize {
//         self.meta.rec_offset()
//     }
//     fn size(&self) -> usize {
//         self.meta.size()
//     }
//     fn precision(&self) -> usize {
//         self.meta.precision()
//     }
//     fn next_id(&mut self) -> u32 {
//         self.meta.next_id()
//     }
// }

// impl<'a> Display for DateTimeField<'a> {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.content)
//     }
// }

// impl<'a> FieldOps for DateTimeField<'a> {

//     fn from_record_bytes(&mut self) -> BoxFuture<()> {
//         Box::pin(
//             async move {
//                 let half : usize = self.meta.rec_offset() + self.meta.size() / 2;
//                 let date_field = &self.record[self.meta.rec_offset()..half];
//                 let time_field = &self.record[half..(self.meta.rec_offset() + self.meta.size())];
//                 let naive_date = NaiveDate::from_num_days_from_ce(i32::from_le_bytes(date_field.try_into().unwrap()) - 1_721_426);
//                 let milli_4_midnight = u32::from_le_bytes(time_field.try_into().unwrap());
//                 self.content = naive_date.and_hms((milli_4_midnight / 3_600_000) % 24, (milli_4_midnight / 60_000) % 60, (milli_4_midnight / 1000) % 60);
//             }
//         )
//     }

//     fn to_bytes(&self) -> BoxFuture<&[u8]> {
//         Box::pin(
//             async move {
//                 &self.record[self.meta.rec_offset()..(self.meta.rec_offset() + self.meta.size())]
//             }    
//         )
//     }

//     fn ready(&self) -> bool {
//         self.ready.is_some()
//     }
// }

// impl<T> RecordOps<T> for Record where T: FieldOps {

// }