use aes::cipher::{KeyIvInit, StreamCipher};
use aes::Aes256;
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

use crate::config::SECRET_KEY;

type HmacSha256 = Hmac<Sha256>;
type Aes256Ctr = ctr::Ctr128BE<Aes256>;

pub fn generate_signature(message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(SECRET_KEY).expect("HMAC can take key of any size");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

pub fn verify_signature(message: &[u8], signature: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(SECRET_KEY).expect("HMAC can take key of any size");
    mac.update(message);
    mac.verify(signature.into()).is_ok()
}

pub fn encrypt_message(message: &[u8]) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let iv: [u8; 16] = rng.gen();
    let mut cipher = Aes256Ctr::new_from_slices(SECRET_KEY, &iv).unwrap();
    let mut ciphertext = message.to_vec();
    cipher.apply_keystream(&mut ciphertext);
    [iv.to_vec(), ciphertext].concat()
}

pub fn decrypt_message(ciphertext: &[u8]) -> Vec<u8> {
    let (iv, ciphertext) = ciphertext.split_at(16);
    let mut cipher = Aes256Ctr::new_from_slices(SECRET_KEY, iv).unwrap();
    let mut decrypted_message = ciphertext.to_vec();
    cipher.apply_keystream(&mut decrypted_message);
    decrypted_message
}
