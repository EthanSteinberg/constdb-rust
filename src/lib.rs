extern crate failure;
extern crate byteorder;
extern crate libc;

use byteorder::LittleEndian;
use byteorder::ByteOrder;

use std::io::Write;
use std::path::Path;
use std::collections::HashMap;

use std::fs::File;
use failure::Error;

use std::ops::Deref;
use std::convert::AsRef;

use std::fmt;
use std::io;

use std::os::unix::io::AsRawFd;
use std::slice;
use std::ptr;

const INT_KEY_TYPE: i32 = 0;
const STR_KEY_TYPE: i32 = 1;

pub struct Writer {
    file: File,
    int_offsets: Vec<(i64, (i64, i64))>,
    str_offsets: Vec<(String, (i64, i64))>,
    current_offset: i64,
    closed: bool
}

impl Writer {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Writer, Error> {
        let file = File::create(path)?;

        let result = Writer {
            file, 
            int_offsets: Vec::new(),
            str_offsets: Vec::new(),
            current_offset: 0,
            closed: false
        };
        Ok(result)
    }

    pub fn add_int(&mut self, key: i64, value: &[u8]) -> Result<(), Error> {
        self.file.write(value)?;
        self.int_offsets.push((key, (self.current_offset, self.current_offset + value.len() as i64)));
        self.current_offset += value.len() as i64;
        Ok(())
    }

    pub fn add_str(&mut self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.file.write(value)?;
        self.str_offsets.push((key.to_string(), (self.current_offset, self.current_offset + value.len() as i64)));
        self.current_offset += value.len() as i64;
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), Error> {
        for (key, offset) in &self.int_offsets {
            let type_value = unsafe { std::mem::transmute::<i32, [u8; std::mem::size_of::<i32>()]>(INT_KEY_TYPE.to_le()) };
            let key_value = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>(key.to_le()) };
            let offset1 = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>(offset.0.to_le()) };
            let offset2 = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>(offset.1.to_le()) };

            self.file.write(&type_value)?;
            self.file.write(&offset1)?;
            self.file.write(&offset2)?;
            self.file.write(&key_value)?;
        }

        for (key, offset) in &self.str_offsets {
            let type_value = unsafe { std::mem::transmute::<i32, [u8; std::mem::size_of::<i32>()]>(STR_KEY_TYPE.to_le()) };
            let offset1 = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>(offset.0.to_le()) };
            let offset2 = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>(offset.1.to_le()) };

            let key_bytes = key.as_bytes();

            let key_len = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>((key_bytes.len() as i64).to_le()) };

            self.file.write(&type_value)?;
            self.file.write(&offset1)?;
            self.file.write(&offset2)?;
            self.file.write(&key_len)?;
            self.file.write(&key_bytes)?;
        }

        let table_offset = unsafe { std::mem::transmute::<i64, [u8; std::mem::size_of::<i64>()]>(self.current_offset.to_le()) };

        self.file.write(&table_offset)?;

        self.closed = true;

        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if !self.closed {
            self.close().unwrap()
        }
    }
}

pub struct Mmap {
    ptr: *mut libc::c_void,
    len: usize,
}

impl Mmap {
    fn new(
        file: &File,
        random_access: bool,
    ) -> io::Result<Mmap> {

        let fd = file.as_raw_fd();
        let len = file.metadata()?.len() as usize;

        unsafe {
            let ptr = libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );

            if random_access {
                libc::madvise(ptr, len, libc::MADV_RANDOM);
            }

            if ptr == libc::MAP_FAILED {
                Err(io::Error::last_os_error())
            } else {
                Ok(Mmap {
                    ptr,
                    len,
                })
            }
        }
    }


    #[inline]
    pub fn ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    #[inline]
    pub fn mut_ptr(&mut self) -> *mut u8 {
        self.ptr as *mut u8
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            assert!(
                libc::munmap(
                    self.ptr,
                    self.len
                ) == 0,
                "unable to unmap mmap: {}",
                io::Error::last_os_error()
            );
        }
    }
}

impl Deref for Mmap {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr as *mut u8, self.len) }
    }
}

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl fmt::Debug for Mmap {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Mmap")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .finish()
    }
}

unsafe impl Sync for Mmap {}
unsafe impl Send for Mmap {}

pub struct ReaderOptions {
    random_access: bool
}

impl Default for ReaderOptions {
    fn default() -> ReaderOptions {
        ReaderOptions {
            random_access: true
        }
    }
}

pub struct Reader {
    map: Mmap,
    int_offsets: HashMap<i64, (usize, usize)>,
    str_offsets: HashMap<String, (usize, usize)>,
}

impl Reader {
    pub fn open<P: AsRef<Path>>(path: P, options: ReaderOptions) -> Result<Reader, Error> {
        let file = File::open(path)?;
        let map = Mmap::new(&file, options.random_access)?;
        let mut int_offsets: HashMap<i64, (usize, usize)> = HashMap::new();
        let mut str_offsets: HashMap<String, (usize, usize)> = HashMap::new();

        let offset_size = std::mem::size_of::<i64>();
        let table_location = map.len() - offset_size;

        let mut current_table_location = LittleEndian::read_i64(&map[table_location .. map.len()]) as usize;

        while current_table_location < table_location {
            let key_type = LittleEndian::read_i32(&map[current_table_location .. ]);
            current_table_location += std::mem::size_of::<i32>();

            let start = LittleEndian::read_i64(&map[current_table_location .. (current_table_location + std::mem::size_of::<i64>())]);
            current_table_location += std::mem::size_of::<i64>();
            
            let end = LittleEndian::read_i64(&map[current_table_location .. (current_table_location + std::mem::size_of::<i64>())]);
            current_table_location += std::mem::size_of::<i64>();

            let key_val = LittleEndian::read_i64(&map[current_table_location .. (current_table_location + std::mem::size_of::<i64>())]);
            current_table_location += std::mem::size_of::<i64>();
        
            let offset = (start as usize, end as usize);

            if key_type == INT_KEY_TYPE {
                int_offsets.insert(key_val, offset);
            } else if key_type == STR_KEY_TYPE {
                let key_bytes = &map[current_table_location .. (current_table_location + key_val as usize)];
                current_table_location += key_val as usize;
              
                let key_str = std::str::from_utf8(key_bytes)?;

                str_offsets.insert(key_str.to_string(), offset);
            }
        }

        let result = Reader {
            map,
            int_offsets,
            str_offsets
        };

        return Ok(result)
    }

    pub fn get_int(&self, key: i64) -> Option<&[u8]> {
        match self.int_offsets.get(&key) {
            Some((start, end)) => Some(&self.map[start.clone() .. end.clone()]),
            None => None
        }
    }

    pub fn get_str(&self, key: &str) -> Option<&[u8]> {
        match self.str_offsets.get(key) {
            Some((start, end)) => Some(&self.map[start.clone() .. end.clone()]),
            None => None
        }
    }
}

#[cfg(test)]
mod tests {

    extern crate tempfile;

    use self::tempfile::TempDir;

    #[test]
    fn test_int() {
        let tmp_dir = TempDir::new().unwrap();

        let path_to_file = tmp_dir.path().join("temp.db");
        
        {
            let mut writer = ::Writer::create(&path_to_file).unwrap();


            writer.add_str("Foo bar", &[1u8, 7u8]).unwrap();
            writer.add_int(43, &[72u8, 101u8, 108u8, 108u8, 111u8]).unwrap();
            writer.add_int(21, &[72u8, 101u8, 32u8, 111u8]).unwrap();
            writer.add_str("Hello world", &[14u8, 21u8]).unwrap();
            writer.add_int(65, &[1u8, 37u8, 121u8]).unwrap();
            writer.close().unwrap(); 
        }
        {
            let reader = ::Reader::open(&path_to_file, ::ReaderOptions::default()).unwrap();
            assert_eq!(reader.get_int(42), None);
            assert_eq!(reader.get_int(43).unwrap(), &[72u8, 101u8, 108u8, 108u8, 111u8]);
            assert_eq!(reader.get_int(65).unwrap(), &[1u8, 37u8, 121u8]);
            assert_eq!(reader.get_str("Hello world").unwrap(), &[14u8, 21u8]);
            assert_eq!(reader.get_str("Foo bar").unwrap(), &[1u8, 7u8]);
        }
    }
}
