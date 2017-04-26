use std::io::prelude::*;
use errors::*;
// use memchr;


#[derive(Debug)]
pub struct Graphite {
    metric: Vec<u8>,
    value: Vec<u8>,
    timestamp: Vec<u8>,
}


// #[inline(always)]
// fn consume_until<R: BufRead + ?Sized>(r: &mut R, delim: u8) -> ::std::io::Result<usize> {
//     let mut read = 0;
//     loop {
//         let (done, used) = {
//             let available = match r.fill_buf() {
//                 Ok(n) => n,
//                 Err(ref e) if e.kind() == ::std::io::ErrorKind::Interrupted => continue,
//                 Err(e) => return Err(e),
//             };
//             match memchr::memchr(delim, available) {
//                 Some(i) => (true, i + 1),
//                 None => (false, available.len()),
//             }
//         };
//         r.consume(used);
//         read += used;
//         if done || used == 0 {
//             return Ok(read);
//         }
//     }
// }


pub fn decode(mut raw_msg: &[u8]) -> Result<Graphite> {
    let mut metric = Vec::with_capacity(100);
    raw_msg.read_until(b' ', &mut metric).chain_err(|| "failed to read metric")?;
    let mut value = Vec::with_capacity(100);
    raw_msg.read_until(b' ', &mut value).chain_err(|| "failed to read value")?;
    let mut timestamp = Vec::with_capacity(100);
    raw_msg.read_until(b'\n', &mut timestamp).chain_err(|| "failed to read timestamp")?;

    // // Skip rest of line
    // if consume_until(&mut f, b'\n')
    //     .chain_err(|| "failed to read byte_count_buf from buffer")? == 0 {
    //     break;
    // }

    // Always assume an empty line after each metric
    // if consume_until(&mut f, b'\n')
    //     .chain_err(|| "failed to read byte_count_buf from buffer")? == 0 {
    //     break;
    // }

    Ok(Graphite {
        metric: metric,
        value: value,
        timestamp: timestamp,
    })
}
