const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;
const EARTH_RADIUS_METER: f64 = 6372797.560856;
pub fn validate(longitude: f64, latitude: f64) -> bool {
    longitude >= MIN_LONGITUDE
        && longitude <= MAX_LONGITUDE
        && latitude >= MIN_LATITUDE
        && latitude <= MAX_LATITUDE
}

// bit dilation with bitshifting technique: abcd -> 0a0b0c0d
// split into 2 halves and move 2 bits (size of each half)
// then or with the input to keep lowest half. & mask to delete the middle
// similarly, take the lowest half, split it to 2 halves again and move 1 bit (size of current half)
// or with the input to keep lowest half and mask to delete the middle
// that happens the same on higher half
//     abcd
//   abcd00
//   abxxcd
//   ab00cd
//  ab00cd0
//  axb0cxd
//  a0b0c0d
// 0a0b0c0d

fn dilate_bits(v: u32) -> u64 {
    let mut v = v as u64;
    v = (v << 16 | v) & 0x0000_FFFF_0000_FFFF;
    v = (v << 8 | v) & 0x00FF_00FF_00FF_00FF;
    v = (v << 4 | v) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v << 2 | v) & 0x3333_3333_3333_3333;
    v = (v << 1 | v) & 0x5555_5555_5555_5555;
    v
}

// reverse bit dilation to turn u64 into u32
// first, mask to have dilated format like 0a0b because the input might be interleaved
// shift right 1 and or and mask again to have 00ab
// xaxb
// 0a0b (mask 0101)
// 00a0 (shift right)
// 0aab (or)
// 00ab (mask 0011)
// similarly, for large span, like 2x, 4x, 8x, we repeatedly shift right from 1 -> 2 -> 4.. and or and mask
// to make the halves compacted into 1 at each step
fn compact_bits(v: u64) -> u32 {
    let mut v = v & 0x5555_5555_5555_5555;
    v = (v >> 1 | v) & 0x3333_3333_3333_3333;
    v = (v >> 2 | v) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v >> 4 | v) & 0x00FF_00FF_00FF_00FF;
    v = (v >> 8 | v) & 0x0000_FFFF_0000_FFFF;
    v = (v >> 16 | v) & 0x0000_0000_FFFF_FFFF;
    v as u32
}

fn interleave(x: u32, y: u32) -> u64 {
    (dilate_bits(x) << 1) | dilate_bits(y)
}

fn deinterleave(code: u64) -> (u32, u32) {
    (compact_bits(code >> 1), compact_bits(code))
}

fn normalize(x: f64, min: f64, max: f64) -> f64 {
    2.0_f64.powi(26) * (x - min) / (max - min)
}

fn denormalize(x: u32, min: f64, max: f64) -> f64 {
    let low = min + (max - min) * (x as f64) / (2.0_f64.powi(26));
    let high = min + (max - min) * (x as f64 + 1.0) / (2.0_f64.powi(26));
    (low + high) / 2.0
}

pub fn encode(longitude: f64, latitude: f64) -> u64 {
    let x = normalize(longitude, MIN_LONGITUDE, MAX_LONGITUDE) as u32;
    let y = normalize(latitude, MIN_LATITUDE, MAX_LATITUDE) as u32;
    interleave(x, y)
}

pub struct GeoPoint {
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoPoint {
    pub fn dist(&self, other: Self) -> f64 {
        let v = ((other.longitude.to_radians() - self.longitude.to_radians()) / 2.0).sin();
        if v == 0.0 {
            return EARTH_RADIUS_METER
                * ((other.latitude.to_radians() - self.latitude.to_radians()).abs());
        }
        let u = ((other.latitude.to_radians() - self.latitude.to_radians()) / 2.0).sin();
        let a =
            u * u + self.latitude.to_radians().cos() * other.latitude.to_radians().cos() * v * v;
        return 2.0 * EARTH_RADIUS_METER * a.sqrt().asin();
    }
}

pub fn decode(code: u64) -> GeoPoint {
    let (x, y) = deinterleave(code);
    GeoPoint {
        longitude: denormalize(x, MIN_LONGITUDE, MAX_LONGITUDE),
        latitude: denormalize(y, MIN_LATITUDE, MAX_LATITUDE),
    }
}
