// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Sample code reference:
// https://github.com/openenclave/openenclave/blob/master/samples/remote_attestation/common/crypto.cpp

#include "mCrypto.h"
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <openenclave/enclave.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

Crypto::Crypto() : m_initialized(false) { m_initialized = init_mbedtls(); }

Crypto::~Crypto() { cleanup_mbedtls(); }

/**
 * init_mbedtls initializes the crypto module.
 * mbedtls initialization. Please refer to mbedtls documentation for detailed
 * information about the functions used.
 */
bool Crypto::init_mbedtls(void) {
  bool ret = false;
  int res = -1;

  if (m_initialized)
    return true;
  m_initialized = true;

  mbedtls_ctr_drbg_init(&m_ctr_drbg_contex);
  mbedtls_entropy_init(&m_entropy_context);
  mbedtls_pk_init(&m_pk_context);

  // Initialize entropy.
  res = mbedtls_ctr_drbg_seed(&m_ctr_drbg_contex, mbedtls_entropy_func, &m_entropy_context, NULL,
                              0);
  if (res != 0) {
    fprintf(stderr, "mbedtls_ctr_drbg_seed failed.\n");
    goto exit;
  }

  // Initialize RSA context.
  res = mbedtls_pk_setup(&m_pk_context, mbedtls_pk_info_from_type(MBEDTLS_PK_RSA));
  if (res != 0) {
    fprintf(stderr, "mbedtls_pk_setup failed (%d).\n", res);
    goto exit;
  }

  // Generate an ephemeral 2048-bit RSA key pair with
  // exponent 65537 for the enclave.
  res = mbedtls_rsa_gen_key(mbedtls_pk_rsa(m_pk_context), mbedtls_ctr_drbg_random,
                            &m_ctr_drbg_contex, 2048, 65537);
  if (res != 0) {
    fprintf(stderr, "mbedtls_rsa_gen_key failed (%d)\n", res);
    goto exit;
  }

  // Write out the public key in PEM format for exchange with other enclaves.
  res = mbedtls_pk_write_pubkey_pem(&m_pk_context, m_public_key, sizeof(m_public_key));
  if (res != 0) {
    fprintf(stderr, "mbedtls_pk_write_pubkey_pem failed (%d)\n", res);
    goto exit;
  }
  ret = true;

exit:
  return ret;
}

/**
 * mbedtls cleanup during shutdown.
 */
void Crypto::cleanup_mbedtls(void) {
  mbedtls_pk_free(&m_pk_context);
  mbedtls_entropy_free(&m_entropy_context);
  mbedtls_ctr_drbg_free(&m_ctr_drbg_contex);
}

/**
 * Get the public key for this enclave.
 */
void Crypto::retrieve_public_key(uint8_t pem_public_key[512]) {
  memcpy(pem_public_key, m_public_key, sizeof(m_public_key));
}

// Compute the sha256 hash of given data.
int Crypto::sha256(const uint8_t *data, size_t data_size, uint8_t sha256[32]) {
  int ret = 0;
  mbedtls_sha256_context ctx;

  mbedtls_sha256_init(&ctx);

  ret = mbedtls_sha256_starts_ret(&ctx, 0);
  if (ret)
    goto exit;

  ret = mbedtls_sha256_update_ret(&ctx, data, data_size);
  if (ret)
    goto exit;

  ret = mbedtls_sha256_finish_ret(&ctx, sha256);
  if (ret)
    goto exit;

exit:
  mbedtls_sha256_free(&ctx);
  return ret;
}

/**
 * Encrypt encrypts the given data using the given public key.
 * Used to encrypt data using the public key of another enclave.
 */
bool Crypto::encrypt(const uint8_t *pem_public_key, const uint8_t *data, size_t data_size,
                     uint8_t *encrypted_data, size_t *encrypted_data_size) {
  bool result = false;
  mbedtls_pk_context key;
  mbedtls_entropy_context entropy;
  mbedtls_ctr_drbg_context ctr_drbg;
  size_t key_size = 0;
  int res = -1;
  mbedtls_rsa_context *rsa_context;

  mbedtls_ctr_drbg_init(&ctr_drbg);
  mbedtls_entropy_init(&entropy);
  mbedtls_pk_init(&key);

  res = mbedtls_ctr_drbg_seed(&ctr_drbg, mbedtls_entropy_func, &entropy, NULL, 0);
  if (res != 0) {
    fprintf(stderr, "mbedtls_ctr_drbg_seed failed.\n");
    goto exit;
  }

  // Read the given public key.
  key_size = strlen((const char *)pem_public_key) + 1; // Include ending '\0'.
  res = mbedtls_pk_parse_public_key(&key, pem_public_key, key_size);
  if (res != 0) {
    fprintf(stderr, "mbedtls_pk_parse_public_key failed.\n");
    goto exit;
  }

  rsa_context = mbedtls_pk_rsa(key);
  rsa_context->padding = MBEDTLS_RSA_PKCS_V15;
  rsa_context->hash_id = MBEDTLS_MD_SHA256;

  // Encrypt the data.
  res = mbedtls_rsa_pkcs1_encrypt(rsa_context, mbedtls_ctr_drbg_random, &ctr_drbg,
                                  MBEDTLS_RSA_PUBLIC, data_size, data, encrypted_data);
  if (res != 0) {
    fprintf(stderr, "mbedtls_rsa_pkcs1_encrypt failed with %d\n", res);
    goto exit;
  }

  *encrypted_data_size = mbedtls_pk_rsa(key)->len;
  result = true;
exit:
  mbedtls_pk_free(&key);
  mbedtls_entropy_free(&entropy);
  mbedtls_ctr_drbg_free(&ctr_drbg);
  return result;
}

/**
 * decrypt the given data using current enclave's private key.
 * Used to receive encrypted data from another enclave.
 */
bool Crypto::decrypt(const uint8_t *encrypted_data, size_t encrypted_data_size, uint8_t *data,
                     size_t *data_size) {
  bool ret = false;
  size_t output_size = 0;
  int res = 0;
  mbedtls_rsa_context *rsa_context;

  if (!m_initialized)
    goto exit;

  mbedtls_pk_rsa(m_pk_context)->len = encrypted_data_size;
  rsa_context = mbedtls_pk_rsa(m_pk_context);
  rsa_context->padding = MBEDTLS_RSA_PKCS_V15;
  rsa_context->hash_id = MBEDTLS_MD_SHA256;

  output_size = *data_size;
  res = mbedtls_rsa_pkcs1_decrypt(rsa_context, mbedtls_ctr_drbg_random, &m_ctr_drbg_contex,
                                  MBEDTLS_RSA_PRIVATE, &output_size, encrypted_data, data,
                                  output_size);
  if (res != 0) {
    fprintf(stderr, "mbedtls_rsa_pkcs1_decrypt failed with -0x%x\n", -res);
    goto exit;
  }
  *data_size = output_size;
  ret = true;

exit:
  return ret;
}
