/*
  Warnings:

  - You are about to drop the column `open_orders` on the `snap` table. All the data in the column will be lost.
  - You are about to drop the column `user` on the `snap` table. All the data in the column will be lost.
  - Added the required column `openOrders` to the `snap` table without a default value. This is not possible if the table is not empty.

*/
-- DropIndex
DROP INDEX "public"."snap_user_key";

-- AlterTable
ALTER TABLE "public"."snap" DROP COLUMN "open_orders",
DROP COLUMN "user",
ADD COLUMN     "openOrders" TEXT NOT NULL;
