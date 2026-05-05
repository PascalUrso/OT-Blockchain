/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public final class AssetTest {

    @Nested
    class Equality {

        @Test
        public void isReflexive() {
            Asset asset = new Asset("doc1", "hello", 3);
            assertThat(asset).isEqualTo(asset);
        }

        @Test
        public void isSymmetric() {
            Asset a = new Asset("doc1", "hello", 3);
            Asset b = new Asset("doc1", "hello", 3);
            assertThat(a).isEqualTo(b);
            assertThat(b).isEqualTo(a);
        }

        @Test
        public void isTransitive() {
            Asset a = new Asset("doc1", "hello", 3);
            Asset b = new Asset("doc1", "hello", 3);
            Asset c = new Asset("doc1", "hello", 3);
            assertThat(a).isEqualTo(b);
            assertThat(b).isEqualTo(c);
            assertThat(a).isEqualTo(c);
        }

        @Test
        public void differentDocIdIsNotEqual() {
            Asset a = new Asset("doc1", "hello", 3);
            Asset b = new Asset("doc2", "hello", 3);
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        public void differentContentIsNotEqual() {
            Asset a = new Asset("doc1", "hello", 3);
            Asset b = new Asset("doc1", "world", 3);
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        public void differentVersionIsNotEqual() {
            Asset a = new Asset("doc1", "hello", 3);
            Asset b = new Asset("doc1", "hello", 4);
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        public void handlesOtherObjects() {
            Asset a = new Asset("doc1", "hello", 3);
            assertThat(a).isNotEqualTo("not an asset");
        }

        @Test
        public void handlesNull() {
            Asset a = new Asset("doc1", "hello", 3);
            assertThat(a).isNotEqualTo(null);
        }
    }

    @Nested
    class Getters {

        @Test
        public void returnsDocId() {
            Asset asset = new Asset("doc42", "", 0);
            assertThat(asset.getDocId()).isEqualTo("doc42");
        }

        @Test
        public void returnsContent() {
            Asset asset = new Asset("doc1", "abc", 1);
            assertThat(asset.getContent()).isEqualTo("abc");
        }

        @Test
        public void returnsVersion() {
            Asset asset = new Asset("doc1", "abc", 7);
            assertThat(asset.getVersion()).isEqualTo(7);
        }
    }

    @Test
    public void toStringContainsFields() {
        Asset asset = new Asset("doc1", "hello", 3);
        assertThat(asset.toString())
                .contains("doc1")
                .contains("hello")
                .contains("3");
    }
}
