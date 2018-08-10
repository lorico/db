package db

import (
	"database/sql"
	//"errors"
	//"os"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

var dbPath = "./lorico.db"
var dbW *sql.DB
var writeChan chan string

/* INIT the SQLITE3 DB
Receives the PATH to DB
NOTES:
	- debuggers must have been created before to initDB
*/
func InitDB(dbPathInit string, dbCreateTables []string) error {
	// Remove DB if exists
	//utils.RemoveFile(dbPath)
	dbPath = dbPathInit
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		//debugError.Println("ERROR: opening the db: ", dbPath, err)
		return err
	}
	//defer db.Close()

	for _, createStmt := range dbCreateTables {

		_, err = db.Exec(createStmt)
		if err != nil {
			//debugError.Println("ERROR: with the create stmt: ", createStmt, err)
			return err
		}
	}

	// Create the write channel
	writeChan = make(chan string, 10)
	go handleWrite(writeChan)

	dbW = db
	return nil
}

func handleWrite(writeChan chan string) {
	for {
		select {
		case stmt := <-writeChan:
			//debugInfo.Println("Received STMT for write: ", stmt)
			if _, err := dbW.Exec(stmt); err != nil {
				fmt.Println("ERROR: with the write stmt: ", stmt, err)
			}

		}
	}
}

// Return the results as a MAP (column oriented, with KEY = COLUMN HEADER)
func Select(stmt string) (resMap map[string][]interface{}, err error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Println("ERROR: opening the db: ", dbPath, err)
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(stmt)
	if err != nil {
		fmt.Println("ERROR: unable to run query: ", stmt, err)
		return nil, err
	}
	defer rows.Close()

	// READ records (headers)
	headers, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// RESULTS INIT
	resMap = make(map[string][]interface{})

	// COLUMNS: from query result
	columns := make([]interface{}, len(headers))
	columnPointers := make([]interface{}, len(headers))
	for i := 0; i < len(headers); i++ {
		columnPointers[i] = &columns[i]
	}
	cnt := 0
	// For EACH ROW
	for rows.Next() {
		// Scan line
		err := rows.Scan(columnPointers...)
		// Check error
		if err != nil {
			return nil, err
		}
		err = rows.Err()
		if err != nil {
			return nil, err
		}

		// APPEND LINE (MANUAL: as the built-in function doesn't work for multi-dim)
		for k, val := range columns {
			resMap[headers[k]] = append(resMap[headers[k]], val)
		}
		cnt++
	}
	return resMap, nil
}

func Write(stmt string) {

	writeChan <- stmt

}

/*
func fileExists(name string, checksum string) (bool, error) {
	stmt := `
	SELECT count(*) FROM files
	WHERE
		lower(FileName) = lower('` + name + `')
		and FileChecksum = '` + checksum + `'
	;
	`

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		debugError.Println("ERROR: opening the db: ", dbPath, err)
		return false, err
	}
	defer db.Close()

	rows, err := db.Query(stmt)
	if err != nil {
		debugError.Println("ERROR: unable to run query: ", stmt, err)
		return false, err
	}
	defer rows.Close()

	// For each record
	cnt := 0
	for rows.Next() {
		if err := rows.Scan(&cnt); err != nil {
			debugError.Println("ERROR: Unable to get a row: ", err)
			return false, err
		}
		debugInfo.Println("SELECT COUNT: ", cnt)
	}
	if err = rows.Err(); err != nil {
		debugError.Println("ERROR: An issue appeared in the scan of the DB query: ", err)
		return false, err
	}

	if cnt == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func getLines(col, value string) ([]map[string]string, error) {
	debugInfo.Println("getLines: ", col, value)
	stmt := ""

	switch col {
	case "FileFid":
		stmt = `
		SELECT FileFid, FileName, MetaFid FROM files
		WHERE
			lower(FileFid) = lower('` + value + `')
		;
		`

	case "MetaFid":
		stmt = `
		SELECT FileFid, FileName, MetaFid FROM files
		WHERE
			lower(MetaFid) = lower('` + value + `')
		;
		`

	case "FileName":
		stmt = `
		SELECT FileFid, FileName, MetaFid FROM files
		WHERE
			lower(FileName) = lower('` + value + `')
		;
		`

	default:
		return nil, errors.New("db: wrong cmd!")
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		debugError.Println("ERROR: opening the db: ", dbPath, err)
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(stmt)
	if err != nil {
		debugError.Println("ERROR: unable to run query: ", stmt, err)
		return nil, err
	}
	defer rows.Close()

	// For each record
	cnt := 0
	//type km map[string]string
	//res := make([]km)
	var res []map[string]string
	for rows.Next() {
		FFid := ""
		FName := ""
		MFid := ""
		tmp := make(map[string]string)

		if err := rows.Scan(&FFid, &FName, &MFid); err != nil {
			debugError.Println("ERROR: Unable to get a row: ", err)
			return nil, err
		}
		tmp["FileFid"] = FFid
		tmp["FileName"] = FName
		tmp["MetaFid"] = MFid
		res = append(res, tmp)
		cnt++
	}
	debugInfo.Println("getLine COUNT: ", cnt)
	if err = rows.Err(); err != nil {
		debugError.Println("ERROR: An issue appeared in the scan of the DB query: ", err)
		return nil, err
	}

	if cnt == 0 {
		return nil, errors.New("DB ERROR: no result!") // should never reach here...
	}

	debugInfo.Println("getLines result: ", res)
	return res, nil
}

func getFids(name, checkSum string) (string, string, error) {
	stmt := `
	SELECT FileFid, MetaFid FROM files
	WHERE
		lower(FileName) = lower('` + name + `')
		and FileChecksum = '` + checkSum + `'
	;
	`

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		debugError.Println("ERROR: opening the db: ", dbPath, err)
		return "", "", err
	}
	defer db.Close()

	rows, err := db.Query(stmt)
	if err != nil {
		debugError.Println("ERROR: unable to run query: ", stmt, err)
		return "", "", err
	}
	defer rows.Close()

	// For each record
	cnt := 0
	FFid := ""
	MFid := ""
	for rows.Next() {
		if err := rows.Scan(&FFid, &MFid); err != nil {
			debugError.Println("ERROR: Unable to get a row: ", err)
			return "", "", err
		}
		cnt++
		debugInfo.Println("getFids COUNT: ", cnt)
	}
	if err = rows.Err(); err != nil {
		debugError.Println("ERROR: An issue appeared in the scan of the DB query: ", err)
		return "", "", err
	}

	return FFid, MFid, nil
}

func getSession(userEmail string) (string, error) {
	stmt := `
	SELECT Session FROM ui
	WHERE
		lower(UserEmail) = lower('` + userEmail + `')
	;
	`

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		debugError.Println("ERROR: opening the db: ", dbPath, err)
		return "", err
	}
	defer db.Close()

	rows, err := db.Query(stmt)
	if err != nil {
		debugError.Println("ERROR: unable to run query: ", stmt, err)
		return "", err
	}
	defer rows.Close()

	// For each record
	cnt := 0
	sess := ""
	for rows.Next() {
		if err := rows.Scan(&sess); err != nil {
			debugError.Println("ERROR: Unable to get a session: ", err)
			return "", err
		}
		cnt++
		debugInfo.Println("getSession COUNT: ", cnt)
	}
	if err = rows.Err(); err != nil {
		debugError.Println("ERROR: An issue appeared in the scan of the DB query: ", err)
		return "", err
	}
	if cnt == 0 {
		debugWarn.Println("WARNING: no saved Session found for this user: ", userEmail)
		return "", errors.New("WARNING: no saved Session found for this user: " + userEmail)
	}

	return sess, nil
}

func saveSession(userEmail, session string) {
	stmt := ""

	res, _ := getSession(userEmail)
	if res != "" {
		stmt = `
			UPDATE ui
			SET Session = '` + session + `'
			WHERE UserEmail = '` + userEmail + `'
		`
	} else {
		stmt = `
		INSERT INTO ui(UserEmail, Session)
		values('` + userEmail + `', '` + session + `')
		;
		`
	}
	writeChan <- stmt
}


func writeMeta(FileFid, FileName, FileCheckSum, MetaFid string) {

	stmt := `
		INSERT INTO files(FileFid, FileName, FileCheckSum, MetaFid)
		values('` + FileFid + `', '` + FileName + `', '` + FileCheckSum + `', '` + MetaFid + `')
		;
	`
	writeChan <- stmt

}

// check if word exists already in DB, return fid, loricoType
func dbWordExists(word string) (string, string, string, error) {
	stmt := `
		SELECT
			Key,
			Fid,
			LoricoType
		FROM master
		WHERE
			lower(Word) = lower('` + word + `')
		;
	`
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		debugError.Println("ERROR: opening the db: ", dbPath, err)
		return "", "", "", err
	}
	defer db.Close()

	rows, err := db.Query(stmt)
	if err != nil {
		debugError.Println("ERROR: unable to run query: ", stmt, err)
		return "", "", "", err
	}
	defer rows.Close()

	// For each record
	cnt := 0
	var key, fid, loricoType string
	for rows.Next() {
		if err := rows.Scan(&key, &fid, &loricoType); err != nil {
			debugError.Println("ERROR: Unable to get a row: ", err)
			return "", "", "", err
		}
		debugInfo.Println("word found: ", fid, loricoType)
		cnt++
	}
	if err = rows.Err(); err != nil {
		debugError.Println("ERROR: An issue appeared in the scan of the DB query: ", err)
		return "", "", "", err
	}

	if cnt == 0 {
		return "", "", "", nil
	}
	if cnt > 1 {
		// AMBIGUOUS ?
		debugInfo.Println("Word is ambiguous, exists multiple times ?", word, "exists: ", cnt, "times... ")
		return key, fid, loricoType, nil
	} else {
		return key, fid, loricoType, nil
	}

}

func dbAddNode(n *node) {
	stmt := `
		INSERT INTO master(Key, Fid, Word, LoricoType)
		values('` + n.Key + `', '` + n.Fid + `', '` + n.Word + `', '` + n.LoricoType + `')
		;
	`
	writeChan <- stmt

}
*/

/* EXAMPLES
// CLEAN if some jobs were stuck (due to a potential previous bug)
	stmt := `
	SELECT *
	FROM jobs
	WHERE isRunning = 1
	;
	`
	_, data, err := utils.RunSQL_inMem_2slices(db, stmt)
	if err != nil {
		utils.Debug(3, "(scheduler.setUpDB) - ERROR: with the cleanup stmt: "+err.Error()+" // "+stmt)
		return nil, err
	}
	if len(data) >= 1 {
		// Some jobs WERE ACTUALLY STUCK !
		}



	stmt := `
	UPDATE jobs
	SET isRunning = 0
	;
	`
	_, err = db.Exec(stmt)
	if err != nil {
		utils.Debug(1, "(scheduler.setUpDB) - ERROR: with the UPDATE stmt: "+err.Error()+" // "+stmt)
		return nil, err
	}

	return db, nil



	// EXAMPLE: SELECT
	rows, err := db.Query("select id, name from foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// EXAMPLE: DELETE
	_, err = db.Exec("delete from foo")
	if err != nil {
		log.Fatal(err)
	}

	// EXAMPLE: INSERT
	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
	}			log.Fatal(err)

*/
