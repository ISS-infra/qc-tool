CREATE OR REPLACE FUNCTION random_between(real, real) RETURNS real AS $$
DECLARE
    start_int ALIAS FOR $1;
    end_int ALIAS FOR $2;
BEGIN
    RETURN random()*(end_int-start_int)+start_int;
END;
$$ LANGUAGE 'plpgsql' STRICT;

/*
random() = real < 1
end_int-start_int = distance
(end_int-start_int)+start_int = between start and end
*/