"use strict";

const fs = require("fs");
const { Client } = require("pg");
const fetch = require('node-fetch');
const path = require('path');

const config = {
    connectionString:
        "postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
    ssl: {
        rejectUnauthorized: true,
        ca: fs.readFileSync(path.join(__dirname, 'postgresql', 'root.crt')).toString(),
    },
};

const client = new Client(config);

async function setupAndInsertData() {
    try {
        await client.connect();
        console.log('Connected successfully');

        const checkTableExistsQuery = `
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'public'
                AND tablename  = 'Vovarama1992_table'
            );
        `;

        const res = await client.query(checkTableExistsQuery);
        const tableExists = res.rows[0].exists;

        if (tableExists) {
            console.log('Table "Vovarama1992_table" already exists.');
        } else {
            const createTableQuery = `
                CREATE TABLE IF NOT EXISTS Vovarama1992_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    data JSONB
                );
            `;
            await client.query(createTableQuery);
            console.log('Table "Vovarama1992_table" created successfully.');
        }

        let continueFetching = true;
        let page = 1;
        const characters_list = [];

        while (continueFetching) {
            const res = await fetch(`https://rickandmortyapi.com/api/character?page=${page}`);
            const json = await res.json();
            const characters = json.results;
            const prepared_to_db = characters.map((char) => ({
                name: char.name,
                data: JSON.stringify(char)
            }));
            characters_list.push(...prepared_to_db);

            continueFetching = !!json.info.next;
            page++;
        }

        const insertQuery = `
            INSERT INTO Vovarama1992_table (name, data)
            VALUES ($1, $2)
        `;

        for (const char of characters_list) {
            await client.query(insertQuery, [char.name, char.data]);
            console.log("Data inserted successfully");
        }

        console.log('All data inserted successfully');
    } catch (err) {
        console.error('Error setting up the database and inserting data', err.stack);
    } finally {
        await client.end();
    }
}

setupAndInsertData();